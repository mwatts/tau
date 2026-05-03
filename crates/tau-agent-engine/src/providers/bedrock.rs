//! AWS Bedrock provider via rig-bedrock.
//!
//! Uses rig's `CompletionModel::stream()` as the LLM transport while
//! tau's agent loop retains full control over tool execution, context
//! building, and session management.

use async_trait::async_trait;

use super::common::send_event;
use crate::provider::{EventReceiver, EventSender, Provider};
use tau_agent_base::types::*;

const API_ID: &str = "bedrock";

pub struct Bedrock;

#[async_trait]
impl Provider for Bedrock {
    fn api_id(&self) -> &str {
        API_ID
    }

    fn needs_api_key(&self) -> bool {
        false
    }

    fn stream(
        &self,
        model: &Model,
        context: &Context,
        options: &StreamOptions,
    ) -> tau_agent_base::Result<EventReceiver> {
        let (tx, rx) = smol::channel::unbounded();

        let model_id = model.id.clone();
        let api_id = model.api.clone();
        let provider_name = model.provider.clone();
        let region = if model.base_url.is_empty() {
            std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into())
        } else {
            model.base_url.clone()
        };
        let context_clone = context.clone();
        let max_tokens = options.max_tokens.unwrap_or(model.max_tokens.min(16384));
        let temperature = options.temperature;

        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    send_error(&tx, &api_id, &provider_name, &model_id, &e.to_string());
                    return;
                }
            };

            if let Err(e) = rt.block_on(run_stream(
                &model_id,
                &api_id,
                &provider_name,
                &region,
                &context_clone,
                max_tokens,
                temperature,
                &tx,
            )) {
                send_error(&tx, &api_id, &provider_name, &model_id, &e.to_string());
            }
        });

        Ok(rx)
    }
}

fn send_error(tx: &EventSender, api_id: &str, provider: &str, model_id: &str, msg: &str) {
    let mut err_msg = AssistantMessage::empty(api_id, provider, model_id);
    err_msg.stop_reason = StopReason::Error;
    err_msg.error_message = Some(msg.to_string());
    let _ = tx.send_blocking(StreamEvent::Error {
        reason: StopReason::Error,
        error: err_msg,
    });
}

async fn run_stream(
    model_id: &str,
    api_id: &str,
    provider_name: &str,
    region: &str,
    context: &Context,
    max_tokens: u64,
    temperature: Option<f64>,
    tx: &EventSender,
) -> tau_agent_base::Result<()> {
    use futures::StreamExt;
    use rig::completion::{CompletionModel as _, GetTokenUsage as _};
    use rig::prelude::*;

    let client = rig_bedrock::client::ClientBuilder::default()
        .region(region)
        .build()
        .await;
    let completion_model = client.completion_model(model_id);

    let request = build_request(context, max_tokens, temperature)?;

    let mut stream = completion_model
        .stream(request)
        .await
        .map_err(|e| tau_agent_base::Error::Http(format!("bedrock stream: {}", e)))?;

    let mut output = AssistantMessage::empty(api_id, provider_name, model_id);
    send_event(tx, StreamEvent::Start { partial: output.clone() })?;

    let mut text_started = false;
    let mut content_index: usize = 0;
    let mut tool_calls_seen = false;
    // Track tool call deltas: map internal_call_id → (tau content_index, id, name, accumulated args)
    let mut tool_state: std::collections::HashMap<String, (usize, String, String, String)> =
        std::collections::HashMap::new();

    while let Some(item) = stream.next().await {
        let chunk = match item {
            Ok(c) => c,
            Err(e) => {
                return Err(tau_agent_base::Error::Http(format!("bedrock chunk: {}", e)));
            }
        };

        match chunk {
            rig::streaming::StreamedAssistantContent::Text(t) => {
                if !text_started {
                    text_started = true;
                    send_event(
                        tx,
                        StreamEvent::TextStart {
                            content_index,
                            partial: output.clone(),
                        },
                    )?;
                }
                send_event(
                    tx,
                    StreamEvent::TextDelta {
                        content_index,
                        delta: t.text.clone(),
                        partial: output.clone(),
                    },
                )?;
            }
            rig::streaming::StreamedAssistantContent::ToolCall {
                tool_call,
                internal_call_id: _,
            } => {
                tool_calls_seen = true;
                // Close text block if open
                if text_started {
                    text_started = false;
                    content_index += 1;
                }
                let tc_index = content_index;
                content_index += 1;

                let tau_tc = ToolCall {
                    id: tool_call.id.clone(),
                    name: tool_call.function.name.clone(),
                    arguments: tool_call.function.arguments.clone(),
                };
                output.content.push(AssistantContent::ToolCall(tau_tc.clone()));

                send_event(
                    tx,
                    StreamEvent::ToolcallStart {
                        content_index: tc_index,
                        partial: output.clone(),
                    },
                )?;
                send_event(
                    tx,
                    StreamEvent::ToolcallEnd {
                        content_index: tc_index,
                        tool_call: tau_tc,
                        partial: output.clone(),
                    },
                )?;
            }
            rig::streaming::StreamedAssistantContent::ToolCallDelta {
                id,
                internal_call_id,
                content,
            } => {
                tool_calls_seen = true;
                let entry = tool_state.entry(internal_call_id.clone()).or_insert_with(|| {
                    if text_started {
                        text_started = false;
                        content_index += 1;
                    }
                    let idx = content_index;
                    content_index += 1;
                    (idx, id.clone(), String::new(), String::new())
                });

                match content {
                    rig::streaming::ToolCallDeltaContent::Name(name) => {
                        entry.2 = name.clone();
                        send_event(
                            tx,
                            StreamEvent::ToolcallStart {
                                content_index: entry.0,
                                partial: output.clone(),
                            },
                        )?;
                    }
                    rig::streaming::ToolCallDeltaContent::Delta(delta) => {
                        entry.3.push_str(&delta);
                        send_event(
                            tx,
                            StreamEvent::ToolcallDelta {
                                content_index: entry.0,
                                delta,
                                partial: output.clone(),
                            },
                        )?;
                    }
                }
            }
            rig::streaming::StreamedAssistantContent::Reasoning(r) => {
                if text_started {
                    text_started = false;
                    content_index += 1;
                }
                let think_idx = content_index;
                content_index += 1;
                let reasoning_text = r
                    .content
                    .iter()
                    .filter_map(|rc| match rc {
                        rig::message::ReasoningContent::Text { text, .. } => Some(text.as_str()),
                        rig::message::ReasoningContent::Summary(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("");
                send_event(
                    tx,
                    StreamEvent::ThinkingStart {
                        content_index: think_idx,
                        partial: output.clone(),
                    },
                )?;
                send_event(
                    tx,
                    StreamEvent::ThinkingEnd {
                        content_index: think_idx,
                        content: reasoning_text,
                        partial: output.clone(),
                    },
                )?;
            }
            rig::streaming::StreamedAssistantContent::ReasoningDelta { reasoning, .. } => {
                send_event(
                    tx,
                    StreamEvent::ThinkingDelta {
                        content_index,
                        delta: reasoning,
                        partial: output.clone(),
                    },
                )?;
            }
            rig::streaming::StreamedAssistantContent::Final(resp) => {
                if let Some(usage) = resp.token_usage() {
                    output.usage.input = usage.input_tokens;
                    output.usage.output = usage.output_tokens;
                    output.usage.recompute_total();
                }
            }
        }
    }

    // Finalize any tool call deltas that never got a full ToolCall event
    for (_internal_id, (tc_idx, id, name, args)) in tool_state {
        let arguments: serde_json::Value =
            serde_json::from_str(&args).unwrap_or(serde_json::Value::Object(Default::default()));
        let tau_tc = ToolCall {
            id,
            name: name.clone(),
            arguments,
        };
        output.content.push(AssistantContent::ToolCall(tau_tc.clone()));
        send_event(
            tx,
            StreamEvent::ToolcallEnd {
                content_index: tc_idx,
                tool_call: tau_tc,
                partial: output.clone(),
            },
        )?;
    }

    // Close text block if still open
    if text_started {
        let text = output
            .content
            .iter()
            .filter_map(|c| match c {
                AssistantContent::Text(t) => Some(t.text.as_str()),
                _ => None,
            })
            .collect::<String>();
        send_event(
            tx,
            StreamEvent::TextEnd {
                content_index,
                content: text,
                partial: output.clone(),
            },
        )?;
    }

    output.stop_reason = if tool_calls_seen {
        StopReason::ToolUse
    } else {
        StopReason::Stop
    };

    send_event(
        tx,
        StreamEvent::Done {
            reason: output.stop_reason,
            message: output,
        },
    )?;

    Ok(())
}

/// Translate tau's Context into rig's CompletionRequest.
fn build_request(
    context: &Context,
    max_tokens: u64,
    temperature: Option<f64>,
) -> tau_agent_base::Result<rig::completion::CompletionRequest> {
    let mut messages: Vec<rig::message::Message> = Vec::new();

    // System prompt as first message
    if let Some(ref sys) = context.system_prompt {
        messages.push(rig::message::Message::system(sys));
    }

    // Convert tau messages → rig messages
    for msg in &context.messages {
        match msg {
            Message::User(u) => {
                let content = user_content_to_rig(u);
                messages.push(rig::message::Message::User { content });
            }
            Message::Assistant(a) => {
                let content = assistant_content_to_rig(a);
                messages.push(rig::message::Message::Assistant {
                    id: a.response_id.clone(),
                    content,
                });
            }
            Message::ToolResult(tr) => {
                let text = tr
                    .content
                    .iter()
                    .map(|c| c.text().to_string())
                    .collect::<Vec<_>>()
                    .join("\n");
                let rig_result = rig::message::UserContent::ToolResult(rig::message::ToolResult {
                    id: tr.tool_call_id.clone(),
                    call_id: None,
                    content: rig::one_or_many::OneOrMany::one(
                        rig::message::ToolResultContent::Text(rig::message::Text { text }),
                    ),
                });
                messages.push(rig::message::Message::User {
                    content: rig::one_or_many::OneOrMany::one(rig_result),
                });
            }
            Message::CompactionSummary(cs) => {
                messages.push(rig::message::Message::user(&cs.summary));
            }
            Message::Info(_) => {}
        }
    }

    // Tools
    let tools: Vec<rig::completion::ToolDefinition> = context
        .tools
        .iter()
        .map(|t| rig::completion::ToolDefinition {
            name: t.name.clone(),
            description: t.description.clone(),
            parameters: t.parameters.clone(),
        })
        .collect();

    // Ensure chat_history is non-empty (rig requires at least one message)
    if messages.is_empty() {
        messages.push(rig::message::Message::user(""));
    }

    Ok(rig::completion::CompletionRequest {
        model: None,
        preamble: None,
        chat_history: rig::one_or_many::OneOrMany::many(messages).unwrap_or_else(|_| {
            rig::one_or_many::OneOrMany::one(rig::message::Message::user(""))
        }),
        documents: vec![],
        tools,
        temperature,
        max_tokens: Some(max_tokens),
        tool_choice: None,
        additional_params: None,
        output_schema: None,
    })
}

fn user_content_to_rig(u: &UserMessage) -> rig::one_or_many::OneOrMany<rig::message::UserContent> {
    let items: Vec<rig::message::UserContent> = u
        .content
        .iter()
        .map(|c| match c {
            UserContent::Text(t) => rig::message::UserContent::text(&t.text),
            UserContent::Image(img) => {
                rig::message::UserContent::text(format!("[image: {}]", img.mime_type))
            }
        })
        .collect();
    if items.is_empty() {
        rig::one_or_many::OneOrMany::one(rig::message::UserContent::text(""))
    } else {
        rig::one_or_many::OneOrMany::many(items)
            .unwrap_or_else(|_| rig::one_or_many::OneOrMany::one(rig::message::UserContent::text("")))
    }
}

fn assistant_content_to_rig(
    a: &AssistantMessage,
) -> rig::one_or_many::OneOrMany<rig::message::AssistantContent> {
    let items: Vec<rig::message::AssistantContent> = a
        .content
        .iter()
        .map(|c| match c {
            AssistantContent::Text(t) => rig::message::AssistantContent::text(&t.text),
            AssistantContent::Thinking(th) => {
                rig::message::AssistantContent::text(format!("<thinking>{}</thinking>", th.thinking))
            }
            AssistantContent::ToolCall(tc) => {
                rig::message::AssistantContent::ToolCall(rig::message::ToolCall {
                    id: tc.id.clone(),
                    call_id: None,
                    function: rig::message::ToolFunction {
                        name: tc.name.clone(),
                        arguments: tc.arguments.clone(),
                    },
                    signature: None,
                    additional_params: None,
                })
            }
        })
        .collect();
    if items.is_empty() {
        rig::one_or_many::OneOrMany::one(rig::message::AssistantContent::text(""))
    } else {
        rig::one_or_many::OneOrMany::many(items).unwrap_or_else(|_| {
            rig::one_or_many::OneOrMany::one(rig::message::AssistantContent::text(""))
        })
    }
}
