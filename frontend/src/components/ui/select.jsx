import { cn } from "@/lib/utils";

export function Select({ className, ...props }) {
  return (
    <select
      className={cn(
        "flex h-10 w-full rounded-xl border border-stone-200 bg-white px-3 py-2 text-sm text-stone-900 outline-none transition focus-visible:ring-2 focus-visible:ring-teal-700",
        className,
      )}
      {...props}
    />
  );
}
