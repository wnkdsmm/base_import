(() => {
    const sidebar = document.querySelector(".sidebar");
    if (!sidebar) {
        return;
    }

    const mobileBreakpoint = 960;
    const openLabel = "Menu";
    const closeLabel = "Close";
    const body = document.body;

    if (!sidebar.id) {
        sidebar.id = "appSidebar";
    }

    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "sidebar-toggle";
    toggle.setAttribute("aria-controls", sidebar.id);

    const overlay = document.createElement("div");
    overlay.className = "sidebar-overlay";
    overlay.setAttribute("aria-hidden", "true");

    const isMobile = () => window.innerWidth <= mobileBreakpoint;

    const normalizePath = (value) => {
        const trimmed = value.replace(/\/+$/, "");
        return trimmed || "/";
    };

    const currentPath = normalizePath(window.location.pathname);
    const navButtons = sidebar.querySelectorAll(".sidebar-actions button");

    navButtons.forEach((button) => {
        const rawHandler = button.getAttribute("onclick") || "";
        const match = rawHandler.match(/location\.href=['"]([^'"]+)['"]/);

        if (!match) {
            return;
        }

        const targetPath = normalizePath(new URL(match[1], window.location.origin).pathname);

        if (targetPath === currentPath) {
            button.classList.add("is-active");
            button.setAttribute("aria-current", "page");
        }
    });
    const syncState = (isOpen) => {
        toggle.textContent = isOpen ? closeLabel : openLabel;
        toggle.setAttribute("aria-expanded", String(isOpen));
        overlay.setAttribute("aria-hidden", String(!isOpen));
    };

    const setOpen = (nextState) => {
        const isOpen = Boolean(nextState) && isMobile();
        body.classList.toggle("sidebar-open", isOpen);
        syncState(isOpen);
    };

    toggle.addEventListener("click", () => {
        setOpen(!body.classList.contains("sidebar-open"));
    });

    overlay.addEventListener("click", () => {
        setOpen(false);
    });

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            setOpen(false);
        }
    });

    window.addEventListener("resize", () => {
        if (!isMobile()) {
            setOpen(false);
        }
    });

    sidebar.addEventListener("click", (event) => {
        const target = event.target;
        if (!isMobile() || !(target instanceof HTMLElement)) {
            return;
        }
        if (target.closest("a, button")) {
            setOpen(false);
        }
    });

    body.append(toggle, overlay);
    syncState(false);
})();
