interface IconProps {
  name: string;
  size?: number | string;
  className?: string;
}

export default function Icon({ name, size = 20, className = "" }: IconProps) {
  return (
    <span
      className={`material-symbols-outlined ${className}`}
      style={{ fontSize: typeof size === "number" ? `${size}px` : size }}
    >
      {name}
    </span>
  );
}
