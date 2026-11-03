// Accessibility repairs that can only happen in the browser, because the markup is created there.
//
// - Verso hangs a tippy tooltip on every code token, and tippy marks an interactive tooltip's
//   reference element `aria-expanded`. On a `<span>` with no role that attribute is not allowed
//   (WCAG 4.1.2), and there are hundreds per page. Verso creates its tooltips in `window.onload`,
//   which runs after this deferred script, so setting tippy's default here reaches every one.
// - Verso's search box is a `role="searchbox"` that carries `aria-expanded` and controls a listbox:
//   that is a combobox, and saying so is what makes the attributes valid.
//
// Each repair warns in the console when what it repairs is not there, so a Verso upgrade that
// changes the markup shows up as a warning instead of a silently unrepaired page.

(function () {
  "use strict";

  if (typeof window.tippy === "function" && typeof window.tippy.setDefaultProps === "function") {
    window.tippy.setDefaultProps({ aria: { content: "auto", expanded: false } });
  } else {
    console.warn("ferrosa a11y: tippy is not loaded; code tooltips keep aria-expanded");
  }

  window.addEventListener("load", () => {
    const box = document.querySelector('[role="searchbox"][aria-controls]');
    if (box === null) {
      console.warn("ferrosa a11y: no search box found to mark as a combobox");
      return;
    }
    box.setAttribute("role", "combobox");
  });
})();
