/**
 * Drag region bar embedded in the top of main content area.
 * For macOS Overlay titleBarStyle, the native traffic lights sit on
 * top of the sidebar. This component provides a drag-handle strip
 * across the top of the main content area so users can drag the window.
 */
export default function TitleBar() {
  return (
    <div
      data-tauri-drag-region
      className="absolute top-0 left-[240px] right-0 h-[52px] z-40"
    />
  );
}
