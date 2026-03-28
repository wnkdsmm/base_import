(() => {
    const sidebar = document.querySelector('.sidebar');
    const toggle = document.querySelector('.sidebar-toggle');
    const overlay = document.querySelector('.sidebar-overlay');

    if (!sidebar || !toggle || !overlay) {
        return;
    }

    const mobileBreakpoint = 1200;
    const openLabel = 'Меню';
    const closeLabel = 'Закрыть';
    const body = document.body;
    const main = document.querySelector('.main');
    const dismissLinks = sidebar.querySelectorAll('.sidebar-dismiss');
    let lastFocusedElement = null;

    if (!sidebar.id) {
        sidebar.id = 'appSidebar';
    }

    if (main && !main.id) {
        main.id = 'page-content';
    }

    const sidebarHash = `#${sidebar.id}`;
    const closeHash = main && main.id ? `#${main.id}` : '#';

    toggle.setAttribute('aria-controls', sidebar.id);
    if (!toggle.getAttribute('href')) {
        toggle.setAttribute('href', sidebarHash);
    }

    overlay.setAttribute('href', closeHash);
    dismissLinks.forEach((link) => {
        link.setAttribute('href', closeHash);
    });

    const isMobile = () => window.innerWidth <= mobileBreakpoint;

    const normalizePath = (value) => {
        const trimmed = value.replace(/\/+$/, '');
        return trimmed || '/';
    };

    const getFocusableItems = () => Array.prototype.filter.call(
        sidebar.querySelectorAll('a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])'),
        (item) => {
            if (!(item instanceof HTMLElement)) {
                return false;
            }
            return item.offsetParent !== null || item === document.activeElement;
        }
    );

    const focusFirstItem = () => {
        const items = getFocusableItems();
        if (items.length) {
            items[0].focus();
        }
    };

    const currentPath = normalizePath(window.location.pathname);
    const navItems = sidebar.querySelectorAll('.sidebar-actions a[href], .sidebar-actions button');

    navItems.forEach((item) => {
        let targetPath = '';

        if (item instanceof HTMLAnchorElement) {
            const href = item.getAttribute('href') || '';
            if (!href || href.startsWith('#')) {
                return;
            }
            targetPath = normalizePath(new URL(href, window.location.origin).pathname);
        } else {
            const rawHandler = item.getAttribute('onclick') || '';
            const match = rawHandler.match(/location\.href=['"]([^'"]+)['"]/);
            if (!match) {
                return;
            }
            targetPath = normalizePath(new URL(match[1], window.location.origin).pathname);
        }

        if (targetPath === currentPath) {
            item.classList.add('is-active');
            item.setAttribute('aria-current', 'page');
        }
    });

    const replaceHash = (nextHash) => {
        if (!window.history || typeof window.history.replaceState !== 'function') {
            if (nextHash) {
                window.location.hash = nextHash;
            }
            return;
        }

        const nextUrl = `${window.location.pathname}${window.location.search}${nextHash}`;
        window.history.replaceState({}, document.title, nextUrl);
    };

    const syncState = (isOpen) => {
        toggle.textContent = isOpen ? closeLabel : openLabel;
        toggle.setAttribute('aria-expanded', String(isOpen));
        overlay.setAttribute('aria-hidden', String(!isOpen));
    };

    const setOpen = (nextState, options = {}) => {
        const shouldOpen = Boolean(nextState) && isMobile();
        const wasOpen = body.classList.contains('sidebar-open');

        if (shouldOpen && document.activeElement instanceof HTMLElement) {
            lastFocusedElement = document.activeElement;
        }

        if (!shouldOpen && window.location.hash === sidebarHash) {
            replaceHash(closeHash === '#' ? '' : closeHash);
        }

        body.classList.toggle('sidebar-open', shouldOpen);
        syncState(shouldOpen);

        if (shouldOpen) {
            window.requestAnimationFrame(focusFirstItem);
            return;
        }

        if (wasOpen && options.restoreFocus !== false) {
            window.requestAnimationFrame(() => {
                if (toggle instanceof HTMLElement) {
                    toggle.focus();
                } else if (lastFocusedElement instanceof HTMLElement) {
                    lastFocusedElement.focus();
                }
                lastFocusedElement = null;
            });
        }
    };

    toggle.addEventListener('click', (event) => {
        if (!isMobile()) {
            return;
        }
        event.preventDefault();
        setOpen(!body.classList.contains('sidebar-open'));
    });

    const handleCloseClick = (event) => {
        if (!isMobile()) {
            return;
        }
        event.preventDefault();
        setOpen(false);
    };

    overlay.addEventListener('click', handleCloseClick);
    dismissLinks.forEach((link) => {
        link.addEventListener('click', handleCloseClick);
    });

    document.addEventListener('keydown', (event) => {
        const isOpen = body.classList.contains('sidebar-open');

        if (event.key === 'Escape' && isOpen) {
            event.preventDefault();
            setOpen(false);
            return;
        }

        if (event.key !== 'Tab' || !isOpen || !isMobile()) {
            return;
        }

        const items = getFocusableItems();
        if (!items.length) {
            return;
        }

        const first = items[0];
        const last = items[items.length - 1];
        const activeElement = document.activeElement;

        if (event.shiftKey && activeElement === first) {
            event.preventDefault();
            last.focus();
            return;
        }

        if (!event.shiftKey && activeElement === last) {
            event.preventDefault();
            first.focus();
        }
    });

    window.addEventListener('resize', () => {
        if (!isMobile()) {
            setOpen(false, { restoreFocus: false });
        }
    });

    sidebar.addEventListener('click', (event) => {
        const target = event.target;
        if (!isMobile() || !(target instanceof HTMLElement)) {
            return;
        }
        if (target.closest('.sidebar-actions a[href], .sidebar-actions button')) {
            setOpen(false, { restoreFocus: false });
        }
    });

    if (window.location.hash === sidebarHash && isMobile()) {
        replaceHash('');
        setOpen(true, { restoreFocus: false });
        return;
    }

    syncState(false);
})();
