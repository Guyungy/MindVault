"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  getFilteredRowModel,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { Settings2 } from "lucide-react";

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
  tableId,
}) {
  const [sorting, setSorting] = useState([]);
  const [columnFilters, setColumnFilters] = useState([]);
  const [pagination, setPagination] = useState({ pageIndex: 0, pageSize });
  const [columnVisibility, setColumnVisibility] = useState({});
  const [columnSizing, setColumnSizing] = useState({});
  const [columnOrder, setColumnOrder] = useState([]);
  const [showColumnSettings, setShowColumnSettings] = useState(false);
  const [dragColumnId, setDragColumnId] = useState("");
  const settingsRef = useRef(null);
  const storageKey = tableId ? `mindvault:datatable:${tableId}` : "";
  const columnIds = useMemo(() => columns.map((column) => resolveColumnId(column)).filter(Boolean), [columns]);

  useEffect(() => {
    if (!storageKey || typeof window === "undefined") return;
    try {
      const raw = window.localStorage.getItem(storageKey);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") {
        if (parsed.columnVisibility && typeof parsed.columnVisibility === "object") {
          setColumnVisibility(parsed.columnVisibility);
        }
        if (parsed.columnSizing && typeof parsed.columnSizing === "object") {
          setColumnSizing(parsed.columnSizing);
        }
        if (Array.isArray(parsed.columnOrder)) {
          setColumnOrder(parsed.columnOrder.filter((item) => columnIds.includes(item)));
        }
      }
    } catch {
      // ignore invalid persisted table settings
    }
  }, [storageKey, columnIds]);

  useEffect(() => {
    setColumnOrder((current) => {
      const seen = new Set(current);
      const next = current.filter((item) => columnIds.includes(item));
      for (const id of columnIds) {
        if (!seen.has(id)) {
          next.push(id);
        }
      }
      return JSON.stringify(next) === JSON.stringify(current) ? current : next;
    });
  }, [columnIds]);

  useEffect(() => {
    if (!storageKey || typeof window === "undefined") return;
    const payload = {
      columnVisibility,
      columnSizing,
      columnOrder,
    };
    window.localStorage.setItem(storageKey, JSON.stringify(payload));
  }, [storageKey, columnVisibility, columnSizing, columnOrder]);

  useEffect(() => {
    if (!showColumnSettings) return undefined;
    const handlePointerDown = (event) => {
      if (settingsRef.current && !settingsRef.current.contains(event.target)) {
        setShowColumnSettings(false);
      }
    };
    window.addEventListener("pointerdown", handlePointerDown);
    return () => window.removeEventListener("pointerdown", handlePointerDown);
  }, [showColumnSettings]);

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      columnFilters,
      pagination,
      columnVisibility,
      columnSizing,
      columnOrder,
    },
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    onColumnVisibilityChange: setColumnVisibility,
    onColumnSizingChange: setColumnSizing,
    onColumnOrderChange: setColumnOrder,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    columnResizeMode: "onChange",
    defaultColumn: {
      minSize: 88,
      size: 180,
      maxSize: 720,
    },
  });

  const filterValue = useMemo(() => {
    if (!filterColumnId) return "";
    return table.getColumn(filterColumnId)?.getFilterValue() ?? "";
  }, [filterColumnId, table, columnFilters]);

  const hideableColumns = table
    .getAllLeafColumns()
    .filter((column) => column.getCanHide?.() !== false);

  return (
    <div className={cn("space-y-3", className)}>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-[var(--border)]/60 pb-2">
        <div className="flex min-w-0 flex-1 items-center gap-3">
          {filterColumnId ? (
            <Input
              className="h-8 max-w-sm"
              placeholder={filterPlaceholder}
              value={filterValue}
              onChange={(event) => table.getColumn(filterColumnId)?.setFilterValue(event.target.value)}
            />
          ) : null}
          <div className="text-xs text-[var(--muted-foreground)]">
            共 {table.getFilteredRowModel().rows.length} 条
          </div>
        </div>
        <div className="relative" ref={settingsRef}>
          <Button variant="outline" size="sm" onClick={() => setShowColumnSettings((current) => !current)}>
            <Settings2 className="mr-1 h-4 w-4" />
            字段显示
          </Button>
          {showColumnSettings ? (
            <div className="absolute right-0 top-10 z-20 w-64 border border-[var(--border)]/80 bg-[var(--background)] p-3 shadow-sm">
              <div className="mb-2 text-xs font-medium text-[var(--muted-foreground)]">显示或隐藏字段</div>
              <div className="max-h-72 space-y-2 overflow-auto">
                {hideableColumns.map((column) => (
                  <label key={column.id} className="flex items-center gap-2 text-xs text-[var(--foreground)]">
                    <input
                      type="checkbox"
                      checked={column.getIsVisible()}
                      onChange={(event) => column.toggleVisibility(event.target.checked)}
                    />
                    <span className="truncate">{getColumnDisplayLabel(column)}</span>
                  </label>
                ))}
              </div>
              <div className="mt-3 border-t border-[var(--border)]/60 pt-2 text-[11px] text-[var(--muted-foreground)]">
                可拖拽表头调整顺序，显示设置会按当前数据表记住。
              </div>
            </div>
          ) : null}
        </div>
      </div>

      <div className="overflow-hidden rounded-[calc(var(--radius)+0.04rem)] border border-[var(--border)]/70 bg-[var(--background)]">
        <Table className="min-w-full w-max" style={{ width: table.getCenterTotalSize() || "100%" }}>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id} className="border-b hover:bg-transparent">
                {headerGroup.headers.map((header) => {
                  const canResize = header.column.getCanResize();
                  return (
                    <TableHead
                      key={header.id}
                      style={{ width: header.getSize() }}
                      className="relative"
                      onDragOver={(event) => {
                        if (!dragColumnId) return;
                        event.preventDefault();
                      }}
                      onDrop={(event) => {
                        if (!dragColumnId) return;
                        event.preventDefault();
                        const targetColumnId = header.column.id;
                        if (!targetColumnId || dragColumnId === targetColumnId) {
                          setDragColumnId("");
                          return;
                        }
                        setColumnOrder((current) => reorderColumns(current.length ? current : columnIds, dragColumnId, targetColumnId));
                        setDragColumnId("");
                      }}
                    >
                      {header.isPlaceholder ? null : (
                        <div className="flex items-center justify-between gap-2">
                          <div
                            className={cn(
                              "truncate",
                              header.column.id === dragColumnId ? "opacity-50" : "",
                            )}
                            draggable
                            onDragStart={() => setDragColumnId(header.column.id)}
                            onDragEnd={() => setDragColumnId("")}
                          >
                            {flexRender(header.column.columnDef.header, header.getContext())}
                          </div>
                          {canResize ? (
                            <div
                              onMouseDown={header.getResizeHandler()}
                              onTouchStart={header.getResizeHandler()}
                              className={cn(
                                "absolute right-0 top-0 h-full w-2 cursor-col-resize select-none bg-transparent transition hover:bg-[var(--border)]",
                                header.column.getIsResizing() ? "bg-[var(--primary)]/25" : "",
                              )}
                            />
                          ) : null}
                        </div>
                      )}
                    </TableHead>
                  );
                })}
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
                      <TableCell key={cell.id} style={{ width: cell.column.getSize() }}>
                        <div className="max-w-full overflow-hidden text-ellipsis whitespace-nowrap">
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </div>
                      </TableCell>
                    ))}
                  </TableRow>
                );
              })
            ) : (
              <TableRow>
                <TableCell colSpan={table.getVisibleLeafColumns().length || columns.length} className="h-24 text-center text-[var(--muted-foreground)]">
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

function getColumnDisplayLabel(column) {
  const header = column.columnDef?.header;
  if (typeof header === "string" && header.trim()) {
    return header;
  }
  return String(column.columnDef?.accessorKey || column.id || "未命名字段");
}

function resolveColumnId(column) {
  return String(column?.id || column?.accessorKey || "").trim();
}

function reorderColumns(columnOrder, sourceId, targetId) {
  const next = [...columnOrder];
  const sourceIndex = next.indexOf(sourceId);
  const targetIndex = next.indexOf(targetId);
  if (sourceIndex < 0 || targetIndex < 0 || sourceIndex === targetIndex) {
    return next;
  }
  const [moved] = next.splice(sourceIndex, 1);
  next.splice(targetIndex, 0, moved);
  return next;
}
