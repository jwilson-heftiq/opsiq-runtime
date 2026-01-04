from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from opsiq_runtime.domain.common.ids import SubjectId, TenantId


@dataclass(frozen=True)
class OrderLineFulfillmentInput:
    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    need_by_date: Optional[date]
    open_quantity: Optional[float]
    projected_available_quantity: Optional[float]
    order_status: Optional[str]
    is_on_hold: Optional[bool]
    release_shortage_qty: Optional[float]
    plant_shortage_qty: Optional[float]
    projected_onhand_qty_eod: Optional[float]
    supply_qty: Optional[float]
    demand_qty: Optional[float]
    partnum: Optional[str]
    customer_id: Optional[str]
    ordernum: Optional[str | int]
    orderline: Optional[int]
    orderrelnum: Optional[int]
    plant: Optional[str]
    warehouse: Optional[str]
    canonical_version: str
    config_version: str

    @staticmethod
    def new(
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime,
        config_version: str,
        canonical_version: str = "v1",
        subject_type: str = "order_line",
        need_by_date: Optional[date] = None,
        open_quantity: Optional[float] = None,
        projected_available_quantity: Optional[float] = None,
        order_status: Optional[str] = None,
        is_on_hold: Optional[bool] = None,
        release_shortage_qty: Optional[float] = None,
        plant_shortage_qty: Optional[float] = None,
        projected_onhand_qty_eod: Optional[float] = None,
        supply_qty: Optional[float] = None,
        demand_qty: Optional[float] = None,
        partnum: Optional[str] = None,
        customer_id: Optional[str] = None,
        ordernum: Optional[str | int] = None,
        orderline: Optional[int] = None,
        orderrelnum: Optional[int] = None,
        plant: Optional[str] = None,
        warehouse: Optional[str] = None,
    ) -> "OrderLineFulfillmentInput":
        return OrderLineFulfillmentInput(
            tenant_id=TenantId(tenant_id),
            subject_type=subject_type,
            subject_id=SubjectId(subject_id),
            as_of_ts=as_of_ts,
            need_by_date=need_by_date,
            open_quantity=open_quantity,
            projected_available_quantity=projected_available_quantity,
            order_status=order_status,
            is_on_hold=is_on_hold,
            release_shortage_qty=release_shortage_qty,
            plant_shortage_qty=plant_shortage_qty,
            projected_onhand_qty_eod=projected_onhand_qty_eod,
            supply_qty=supply_qty,
            demand_qty=demand_qty,
            partnum=partnum,
            customer_id=customer_id,
            ordernum=ordernum,
            orderline=orderline,
            orderrelnum=orderrelnum,
            plant=plant,
            warehouse=warehouse,
            canonical_version=canonical_version,
            config_version=config_version,
        )

