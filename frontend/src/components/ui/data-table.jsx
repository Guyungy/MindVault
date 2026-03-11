"use client";

import { useMemo, useState } from "react";
import {
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";

export function DataTable({
  columns,
  data,
  filterPlaceholder = "搜索...",
  filterColumnId,
  emptyText = "暂无数据。",
  pageSize = 10,
  className,
  onRowClick,
  selectedRowId,
}) {
  const [sorting, setSorting] = useState([]);
  const [columnFilters, setColumnFilters] = useState([]);
  const [pagination, setPagination] = useState({ pageIndex: 0, pageSize });

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      columnFilters,
      pagination,
    },
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
  });

  const filterValue = useMemo(() => {
    if (!filterColumnId) return "";
    return table.getColumn(filterColumnId)?.getFilterValue() ?? "";
  }, [filterColumnId, table, columnFilters]);

  return (
    <div className={cn("space-y-3", className)}>
      {filterColumnId ? (
        <div className="flex items-center justify-between gap-3 border-b border-[var(--border)]/60 pb-2">
          <Input
            className="h-8 max-w-sm"
            placeholder={filterPlaceholder}
            value={filterValue}
            onChange={(event) => table.getColumn(filterColumnId)?.setFilterValue(event.target.value)}
          />
          <div className="text-xs text-[var(--muted-foreground)]">
            共 {table.getFilteredRowModel().rows.length} 条
          </div>
        </div>
      ) : null}

      <div className="overflow-hidden rounded-[calc(var(--radius)+0.04rem)] border border-[var(--border)]/70 bg-[var(--background)]">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id} className="border-b hover:bg-transparent">
                {headerGroup.headers.map((header) => (
                  <TableHead key={header.id}>
                    {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.length ? (
              table.getRowModel().rows.map((row) => {
                const rowId = row.original?.id || row.original?.entity_id || row.id;
                const isSelected = selectedRowId && rowId === selectedRowId;
                return (
                  <TableRow
                    key={row.id}
                    data-state={isSelected ? "selected" : undefined}
                    className={onRowClick ? "cursor-pointer" : undefined}
                    onClick={() => onRowClick?.(row.original)}
                  >
                    {row.getVisibleCells().map((cell) => (
                      <TableCell key={cell.id}>
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </TableCell>
                    ))}
                  </TableRow>
                );
              })
            ) : (
              <TableRow>
                <TableCell colSpan={columns.length} className="h-24 text-center text-[var(--muted-foreground)]">
                  {emptyText}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      <div className="flex items-center justify-between gap-3 border-t border-[var(--border)]/60 pt-2">
        <div className="text-xs text-[var(--muted-foreground)]">
          第 {table.getState().pagination.pageIndex + 1} / {table.getPageCount() || 1} 页
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={() => table.previousPage()} disabled={!table.getCanPreviousPage()}>
            上一页
          </Button>
          <Button variant="outline" size="sm" onClick={() => table.nextPage()} disabled={!table.getCanNextPage()}>
            下一页
          </Button>
        </div>
      </div>
    </div>
  );
}
