(function() {
  'use strict';
  var state = { ws: null, sessions: [], activeSession: null };

  function connect() {
    var token = new URLSearchParams(window.location.search).get('token') || '';
    var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    var ws = new WebSocket(proto + '//' + location.host + '/ws?token=' + token);
    ws.onopen = function() { console.log('connected'); };
    ws.onmessage = function(e) { handleMessage(JSON.parse(e.data)); };
    ws.onclose = function() { console.log('disconnected'); setTimeout(connect, 3000); };
    ws.onerror = function(e) { console.error('ws error', e); };
    state.ws = ws;
  }

  function send(requestId, request) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(JSON.stringify({ request_id: requestId, request: request }));
    }
  }

  function handleMessage(msg) {
    if (msg.event === 'connected') {
      appendChat('system', 'Connected to tau daemon');
      return;
    }
    if (msg.echo) {
      appendChat('system', msg.echo);
    }
  }

  function appendChat(role, text) {
    var view = document.getElementById('messages');
    var div = document.createElement('div');
    div.className = 'message ' + role;
    div.textContent = text;
    view.appendChild(div);
    view.scrollTop = view.scrollHeight;
  }

  // Input handling
  function setupInput() {
    var input = document.getElementById('input');
    if (!input) return;
    input.addEventListener('keydown', function(e) {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        var text = input.value.trim();
        if (text) {
          appendChat('user', text);
          send('msg-' + Date.now(), { Chat: { session_id: state.activeSession || '', text: text, attachments: [] } });
          input.value = '';
        }
      }
    });
  }

  // Nav buttons
  function setupNav() {
    var buttons = document.querySelectorAll('.nav-btn');
    buttons.forEach(function(btn) {
      btn.addEventListener('click', function() {
        buttons.forEach(function(b) { b.classList.remove('active'); });
        btn.classList.add('active');
      });
    });
  }

  document.addEventListener('DOMContentLoaded', function() {
    setupNav();
    setupInput();
    connect();
  });
})();
