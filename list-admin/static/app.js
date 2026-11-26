/**
 * Shared page behaviour, wired up with event listeners instead of inline on*="..." attributes.
 * Inline handlers are blocked by the Content-Security-Policy and building JavaScript strings from template data inside HTML attributes is an XSS risk.
 * 
 *   data-open-modal="id"     click opens the modal with that id
 *   data-close-modal="id"    click closes it
 *   data-stop-propagation    clicks inside (for example the modal box) don't reach the backdrop
 *   data-autosubmit          change submits the element's form (language/list selectors)
 *   <form data-confirm="..."> asks for confirmation before submitting
 */
(function () {
    "use strict";

    if (typeof window.openModal !== "function") {
        window.openModal = function (id) {
            const modal = document.getElementById(id);
            if (!modal) return;
            modal.classList.add("active");
            document.body.classList.add("modal-open");
        };
    }
    if (typeof window.closeModal !== "function") {
        window.closeModal = function (id) {
            const modal = document.getElementById(id);
            if (modal) modal.classList.remove("active");
            if (!document.querySelector(".modal.active")) {
                document.body.classList.remove("modal-open");
            }
        };
    }

    function bind() {
        //Direct listeners so the order matches the old inline handlers:
        //a close button inside a modal box runs first, then the box stops the click before it reaches the backdrop.
        document.querySelectorAll("[data-open-modal]").forEach((el) => {
            el.addEventListener("click", () => window.openModal(el.dataset.openModal));
        });
        document.querySelectorAll("[data-close-modal]").forEach((el) => {
            el.addEventListener("click", () => window.closeModal(el.dataset.closeModal));
        });
        document.querySelectorAll("[data-stop-propagation]").forEach((el) => {
            el.addEventListener("click", (event) => event.stopPropagation());
        });
        document.querySelectorAll("[data-autosubmit]").forEach((el) => {
            el.addEventListener("change", () => {
                if (el.form) el.form.submit();
            });
        });
        document.querySelectorAll("form[data-confirm]").forEach((form) => {
            form.addEventListener("submit", (event) => {
                if (!window.confirm(form.dataset.confirm)) event.preventDefault();
            });
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", bind);
    } else {
        bind();
    }
})();
