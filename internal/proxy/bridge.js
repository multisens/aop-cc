// Bridge of the platform in every HTML document of a broadcaster application:
// the keys, the media time and the stream events the platform sends by
// postMessage are dispatched on the document (ABNT NBR 25608, B.5.1). A
// BrowserBack that no handler cancels goes back to the platform.
(function () {
    const EVENT_TYPE = 'tv30-aop-event';
    const BACK_TYPE = 'tv30-aop-back';
    const BACK_KEY = 'BrowserBack';

    // The key on the focused element, or on the document without one.
    function dispatchKey(key, platformOrigin) {
        const target = document.activeElement || document;
        const event = new KeyboardEvent('keydown', { key: key, bubbles: true, cancelable: true });
        const notCanceled = target.dispatchEvent(event);
        if (key === BACK_KEY && notCanceled) {
            window.parent.postMessage({ type: BACK_TYPE }, platformOrigin);
        }
    }

    function onMessage(e) {
        if (e.source !== window.parent) {
            return;
        }
        const message = e.data;
        if (!message || typeof message !== 'object' || message.type !== EVENT_TYPE) {
            return;
        }
        switch (message.event) {
            case 'keydown':
                if (typeof message.key === 'string') {
                    dispatchKey(message.key, e.origin);
                }
                break;
            case 'timeupdate':
            case 'stream_event':
                document.dispatchEvent(new CustomEvent(message.event, { detail: message.detail }));
                break;
            default:
                break;
        }
    }

    if (window.parent !== window) {
        window.addEventListener('message', onMessage);
    }
})();
