import { Slot } from "@radix-ui/react-slot";
import { cva } from "class-variance-authority";

import { cn } from "@/lib/utils";

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-1.5 whitespace-nowrap rounded-[calc(var(--radius)-0.02rem)] text-[11px] font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)] disabled:pointer-events-none disabled:opacity-50",
  {
    variants: {
      variant: {
        default:
          "bg-[var(--primary)] text-[var(--primary-foreground)] hover:opacity-92",
        outline:
          "border border-[var(--border)]/70 bg-[var(--background)] text-[var(--foreground)] hover:bg-[var(--accent)] hover:text-[var(--accent-foreground)]",
        ghost:
          "text-[var(--muted-foreground)] hover:bg-[var(--accent)] hover:text-[var(--accent-foreground)]",
        nav:
          "w-full justify-start rounded-[calc(var(--radius)-0.02rem)] bg-transparent text-[var(--sidebar-foreground)]/72 hover:bg-[var(--sidebar-accent)] hover:text-[var(--sidebar-accent-foreground)]",
        activeNav:
          "w-full justify-start rounded-[calc(var(--radius)-0.02rem)] bg-[var(--sidebar-accent)] text-[var(--sidebar-primary)]",
      },
      size: {
        default: "h-8 px-3 py-1.5",
        sm: "h-6 rounded-[calc(var(--radius)-0.02rem)] px-2 text-[10px]",
        lg: "h-9 px-4",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

export function Button({ className, variant, size, asChild = false, ...props }) {
  const Comp = asChild ? Slot : "button";
  return <Comp className={cn(buttonVariants({ variant, size, className }))} {...props} />;
}

export { buttonVariants };
