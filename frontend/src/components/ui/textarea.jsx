import { cn } from "@/lib/utils";

export function Textarea({ className, ...props }) {
  return (
    <textarea
      className={cn(
        "flex min-h-24 w-full rounded-xl border border-stone-200 bg-white px-3 py-3 text-sm text-stone-900 shadow-sm transition-colors outline-none placeholder:text-stone-400 focus-visible:ring-2 focus-visible:ring-teal-700 disabled:cursor-not-allowed disabled:opacity-50",
        className,
      )}
      {...props}
    />
  );
}
