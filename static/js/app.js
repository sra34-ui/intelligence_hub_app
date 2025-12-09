// Chat application logic
const chatMessages = document.getElementById('chatMessages');
const chatSection = document.getElementById('chatSection');
const chatForm = document.getElementById('chatForm');
const chatFormBottom = document.getElementById('chatFormBottom');
const messageInput = document.getElementById('messageInput');
const messageInputBottom = document.getElementById('messageInputBottom');
const sendButton = document.getElementById('sendButton');
const sendButtonBottom = document.getElementById('sendButtonBottom');
const clearButton = document.getElementById('clearButton');
const suggestionChips = document.querySelectorAll('.suggestion-chip');

// Auto-resize textarea (for bottom chat input)
if (messageInputBottom) {
    messageInputBottom.addEventListener('input', function() {
        this.style.height = 'auto';
        this.style.height = Math.min(this.scrollHeight, 120) + 'px';
    });
}

// Handle form submission from hero search
if (chatForm) {
    chatForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        await handleMessage(messageInput.value.trim());
    });
}

// Handle form submission from bottom chat (if visible)
if (chatFormBottom) {
    chatFormBottom.addEventListener('submit', async (e) => {
        e.preventDefault();
        await handleMessage(messageInputBottom.value.trim());
    });
}

// Main message handling function
async function handleMessage(message) {
    if (!message) return;

    // Show chat section if hidden (only on home page)
    if (chatSection && chatSection.style.display === 'none') {
        chatSection.style.display = 'block';
        // Smooth scroll to chat section
        setTimeout(() => {
            chatSection.scrollIntoView({ behavior: 'smooth' });
        }, 100);
    }

    // Add user message to chat
    addMessage(message, 'user');

    // Clear inputs
    if (messageInput) {
        messageInput.value = '';
    }
    if (messageInputBottom) {
        messageInputBottom.value = '';
        messageInputBottom.style.height = 'auto';
    }

    // Disable inputs while processing
    if (messageInput) {
        messageInput.disabled = true;
    }
    if (sendButton) {
        sendButton.disabled = true;
    }
    if (messageInputBottom) {
        messageInputBottom.disabled = true;
        sendButtonBottom.disabled = true;
    }

    // Show typing indicator with processing message
    const typingId = addTypingIndicator();

    // Add processing message
    const processingMsg = document.createElement('div');
    processingMsg.id = 'processing-msg';
    processingMsg.className = 'message bot-message';
    processingMsg.innerHTML = '<div class="message-content"><em>Analyzing your query with our multi-agent system...</em></div>';
    chatMessages.appendChild(processingMsg);
    chatMessages.scrollTop = chatMessages.scrollHeight;

    try {
        // Send message to backend
        const response = await fetch('/api/chat', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ message }),
        });

        console.log('Response status:', response.status);
        console.log('Response headers:', response.headers);

        // Get the response text first
        const responseText = await response.text();
        console.log('Response text:', responseText.substring(0, 500));

        // Remove typing indicator and processing message
        removeTypingIndicator(typingId);
        const processingMsgEl = document.getElementById('processing-msg');
        if (processingMsgEl) processingMsgEl.remove();

        // Try to parse as JSON
        let data;
        try {
            data = JSON.parse(responseText);
        } catch (parseError) {
            addMessage(`Error: Received non-JSON response (status ${response.status}):\n\n${responseText.substring(0, 1000)}`, 'bot');
            return;
        }

        // Check if there's an error in the response
        if (data.error) {
            console.error('Backend error:', data.error);
            console.error('Error details:', data.details);
            addMessage(`Error: ${data.error}\n\nDetails: ${data.details || 'No additional details'}`, 'bot');
        } else if (data.response) {
            // Add bot response
            addMessage(data.response, 'bot');
        } else {
            addMessage('Unexpected response format', 'bot');
            console.log('Response data:', data);
        }

    } catch (error) {
        console.error('Error:', error);
        removeTypingIndicator(typingId);
        const processingMsgEl = document.getElementById('processing-msg');
        if (processingMsgEl) processingMsgEl.remove();
        addMessage(`Sorry, I encountered an error: ${error.message}\n\nStack: ${error.stack}`, 'bot');
    } finally {
        // Re-enable inputs
        if (messageInput) {
            messageInput.disabled = false;
        }
        if (sendButton) {
            sendButton.disabled = false;
        }
        if (messageInputBottom) {
            messageInputBottom.disabled = false;
            sendButtonBottom.disabled = false;
            messageInputBottom.focus();
        } else if (messageInput) {
            messageInput.focus();
        }
    }
}

// Handle suggestion chips
if (messageInput && chatForm) {
    suggestionChips.forEach(chip => {
        chip.addEventListener('click', () => {
            messageInput.value = chip.textContent;
            messageInput.focus();
            // Trigger submit
            chatForm.dispatchEvent(new Event('submit'));
        });
    });
}

// Handle clear button
if (clearButton) {
    clearButton.addEventListener('click', async () => {
        if (!confirm('Are you sure you want to clear the conversation history?')) {
            return;
        }

        try {
            await fetch('/api/clear', { method: 'POST' });

            // Clear messages
            chatMessages.innerHTML = '';

            // Hide chat section if it exists
            if (chatSection) {
                chatSection.style.display = 'none';
            }

            // Scroll back to top
            window.scrollTo({ top: 0, behavior: 'smooth' });
        } catch (error) {
            console.error('Error clearing session:', error);
        }
    });
}

// Add message to chat
function addMessage(content, type) {
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${type}-message`;

    const contentDiv = document.createElement('div');
    contentDiv.className = 'message-content';

    // Convert markdown-like formatting
    let formattedContent = content
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        .replace(/\*(.*?)\*/g, '<em>$1</em>')
        .replace(/`(.*?)`/g, '<code>$1</code>')
        .replace(/\n/g, '<br>');

    contentDiv.innerHTML = formattedContent;
    messageDiv.appendChild(contentDiv);
    chatMessages.appendChild(messageDiv);

    // Scroll to bottom
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

// Add typing indicator
function addTypingIndicator() {
    const id = 'typing-' + Date.now();
    const messageDiv = document.createElement('div');
    messageDiv.className = 'message bot-message';
    messageDiv.id = id;

    const typingDiv = document.createElement('div');
    typingDiv.className = 'typing-indicator';
    typingDiv.innerHTML = `
        <div class="typing-dot"></div>
        <div class="typing-dot"></div>
        <div class="typing-dot"></div>
    `;

    messageDiv.appendChild(typingDiv);
    chatMessages.appendChild(messageDiv);

    // Scroll to bottom
    chatMessages.scrollTop = chatMessages.scrollHeight;

    return id;
}

// Remove typing indicator
function removeTypingIndicator(id) {
    const element = document.getElementById(id);
    if (element) {
        element.remove();
    }
}

// Enable enter to send, shift+enter for new line (for hero search)
if (messageInput && chatForm) {
    messageInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            chatForm.dispatchEvent(new Event('submit'));
        }
    });
}

// Enable enter to send, shift+enter for new line (for bottom chat)
if (messageInputBottom) {
    messageInputBottom.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            chatFormBottom.dispatchEvent(new Event('submit'));
        }
    });
}
