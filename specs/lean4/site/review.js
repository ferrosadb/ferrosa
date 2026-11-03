// Review ticks for the theorem register.
//
// On a register page each theorem is a list item that opens with a link to the theorem on its
// module page. Verso emits a module doc's list items as `<p>` directly inside the `<ul>`, and
// `postprocess.py` turns them into `<li>`; an item here may be either, so a page built without that
// pass still works. This script puts a checkbox in front of each one and remembers it in localStorage
// under `ferrosa-spec-reviewed:<anchor>`, so a reviewer can work through the register over several
// sittings. Ticks live in this browser only; nothing is sent anywhere. A line above each module's
// list, and one at the top of the page, counts what has been reviewed.

(function () {
  "use strict";

  const PREFIX = "ferrosa-spec-reviewed:";

  function storage() {
    try {
      const probe = PREFIX + "__probe";
      window.localStorage.setItem(probe, "1");
      window.localStorage.removeItem(probe);
      return window.localStorage;
    } catch (err) {
      console.warn("ferrosa review: localStorage unavailable, ticks will not persist", err);
      return null;
    }
  }

  // The theorem link that opens a register item, or null for any other list item.
  function theoremLink(item) {
    const first = item.querySelector(":scope > a, :scope > p > a");
    if (!first) return null;
    const href = first.getAttribute("href") || "";
    return href.startsWith("Ferrosa/") && href.includes("#") ? first : null;
  }
  function wire(store) {
    if (!window.location.pathname.includes("/Register/Part")) return;

    const lists = Array.from(document.querySelectorAll("ul")).filter((list) =>
      Array.from(list.children).some((item) => theoremLink(item) !== null)
    );
    if (lists.length === 0) {
      console.warn("ferrosa review: register page has no theorem lists; markup changed?");
      return;
    }

    const pageCounter = document.createElement("p");
    pageCounter.className = "ferrosa-review-count ferrosa-review-total";
    // A status region, so a screen reader hears the new total after each tick.
    pageCounter.setAttribute("role", "status");
    // The page total goes above the first module's heading, not between it and its list.
    let firstHeading = lists[0];
    for (let el = lists[0].previousElementSibling; el !== null; el = el.previousElementSibling) {
      if (/^H[2-6]$/.test(el.tagName)) { firstHeading = el; break; }
    }
    firstHeading.parentNode.insertBefore(pageCounter, firstHeading);

    const boxes = [];
    const groups = lists.map((list) => {
      const counter = document.createElement("p");
      counter.className = "ferrosa-review-count";
      list.parentNode.insertBefore(counter, list);
      list.classList.add("ferrosa-register");
      const own = [];
      Array.from(list.children).forEach((item) => {
        const link = theoremLink(item);
        if (link === null) return;
        const key = PREFIX + link.getAttribute("href").split("#")[1];
        const box = document.createElement("input");
        box.type = "checkbox";
        box.setAttribute("aria-label", "Reviewed " + link.textContent.trim());
        box.checked = store !== null && store.getItem(key) === "1";
        box.addEventListener("change", () => {
          if (store !== null) {
            if (box.checked) store.setItem(key, "1");
            else store.removeItem(key);
          }
          item.classList.toggle("ferrosa-reviewed", box.checked);
          refresh();
        });
        item.classList.toggle("ferrosa-reviewed", box.checked);
        item.prepend(box);
        own.push(box);
        boxes.push(box);
      });
      return { counter, own };
    });

    function refresh() {
      groups.forEach(({ counter, own }) => {
        const done = own.filter((b) => b.checked).length;
        counter.textContent = done + " of " + own.length + " reviewed";
      });
      const done = boxes.filter((b) => b.checked).length;
      pageCounter.textContent =
        "This part: " + done + " of " + boxes.length + " theorems reviewed" +
        (store === null ? " (ticks will not be kept: this browser blocks storage)" : "");
    }
    refresh();
  }

  document.addEventListener("DOMContentLoaded", () => wire(storage()));
})();
