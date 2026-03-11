import { cva } from "class-variance-authority";

import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center whitespace-nowrap rounded-[var(--radius)] px-2 py-1 text-xs font-medium",
  {
    variants: {
      variant: {
        default: "bg-[var(--primary)]/12 text-[var(--primary)]",
        outline: "border bg-transparent text-[var(--muted-foreground)]",
        warm: "bg-[var(--chart-2)]/12 text-[var(--chart-2)]",
        danger: "bg-[var(--destructive)]/12 text-[var(--destructive)]",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

export function Badge({ className, variant, ...props }) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />;
}
