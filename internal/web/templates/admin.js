document.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-copy-target]");
  if (!button) return;

  const target = document.getElementById(button.dataset.copyTarget);
  if (!target) return;
  const text = "value" in target ? target.value : target.textContent;
  const originalLabel = button.querySelector("span").textContent;

  try {
    await navigator.clipboard.writeText(text.trim());
    button.querySelector("span").textContent = "Copied";
    window.setTimeout(() => { button.querySelector("span").textContent = originalLabel; }, 1500);
  } catch {
    button.querySelector("span").textContent = "Copy failed";
  }
});
