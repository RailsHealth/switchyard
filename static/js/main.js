document.addEventListener('DOMContentLoaded', () => {
    const streamsList = document.getElementById('streams-list');
    const searchInput = document.getElementById('search-streams');
    const orgSelect = document.getElementById('org-select');
    const userDropdown = document.querySelector('.user-dropdown');

    // Organization switcher
    if (orgSelect) {
        orgSelect.addEventListener('change', async () => {
            const selectedOrgId = orgSelect.value;
            try {
                const response = await fetch('/organizations/switch/' + selectedOrgId, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-CSRFToken': getCookie('csrf_token')
                    }
                });
                if (response.ok) {
                    window.location.reload();
                } else {
                    console.error('Failed to switch organization');
                }
            } catch (error) {
                console.error('Error:', error);
            }
        });
    }

    // User dropdown toggle
    if (userDropdown) {
        const userIcon = document.querySelector('.user-icon');
        userIcon.addEventListener('click', () => {
            userDropdown.classList.toggle('active');
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!userDropdown.contains(e.target) && !userIcon.contains(e.target)) {
                userDropdown.classList.remove('active');
            }
        });
    }

    if (streamsList) {
        // Use event delegation for stream actions
        streamsList.addEventListener('click', async (e) => {
            if (e.target.classList.contains('stream-action')) {
                const streamCard = e.target.closest('.stream-card');
                const streamId = streamCard.dataset.streamId;
                const action = e.target.dataset.action;
                try {
                    const response = await fetch(`/streams/${streamId}/${action}`, { method: 'POST' });
                    const result = await response.json();
                    if (result.status === 'success') {
                        location.reload();
                    } else {
                        alert(`Failed to ${action} stream: ${result.message}`);
                    }
                } catch (error) {
                    console.error('Error:', error);
                    alert(`An error occurred while trying to ${action} the stream.`);
                }
            } else if (e.target.classList.contains('delete-stream')) {
                const streamCard = e.target.closest('.stream-card');
                const streamId = streamCard.dataset.streamId;
                if (confirm('Are you sure you want to delete this stream?')) {
                    try {
                        const response = await fetch(`/streams/${streamId}`, { method: 'DELETE' });
                        const result = await response.json();
                        if (result.status === 'success') {
                            streamCard.remove();
                        } else {
                            alert(`Failed to delete stream: ${result.message}`);
                        }
                    } catch (error) {
                        console.error('Error:', error);
                        alert('An error occurred while trying to delete the stream.');
                    }
                }
            } else if (e.target.closest('.stream-card')) {
                // Navigate to stream detail page
                const streamCard = e.target.closest('.stream-card');
                const streamId = streamCard.dataset.streamId;
                window.location.href = `/streams/${streamId}`;
            }
        });
    }

    // Fetch and display messages/logs
    const fetchData = async (url, containerSelector) => {
        try {
            const response = await fetch(url);
            const data = await response.json();
            const container = document.querySelector(containerSelector);
            if (container) {
                container.innerHTML = ''; // Clear existing content
                data.forEach(item => {
                    const element = document.createElement('div');
                    element.classList.add(containerSelector === '#messages-list' ? 'message-item' : 'log-item');
                    element.textContent = JSON.stringify(item, null, 2);
                    container.appendChild(element);
                });
            }
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    };

    // Fetch messages and logs on respective pages
    if (document.querySelector('#messages-list')) {
        fetchData('/api/messages', '#messages-list');
    }
    if (document.querySelector('#logs-list')) {
        fetchData('/api/logs', '#logs-list');
    }

    // Search functionality for streams
    if (searchInput) {
        searchInput.addEventListener('input', () => {
            const searchTerm = searchInput.value.toLowerCase();
            document.querySelectorAll('.stream-card').forEach(card => {
                const streamName = card.querySelector('h3').textContent.toLowerCase();
                card.style.display = streamName.includes(searchTerm) ? '' : 'none';
            });
        });
    }
});

// Helper function to get CSRF token from cookies
function getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(';').shift();
}