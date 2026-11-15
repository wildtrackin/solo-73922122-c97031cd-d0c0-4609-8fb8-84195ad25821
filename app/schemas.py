from typing import Literal

from pydantic import BaseModel, Field


class SampleIn(BaseModel):
    # 两值必填且非空（与旧表 NOT NULL 约束一致）
    temp: float = Field(allow_inf_nan=False)
    ph: float = Field(allow_inf_nan=False)


class SampleOut(BaseModel):
    id: int
    temp: float
    ph: float


class MergeIn(BaseModel):
    """网页只提交“保留哪个编号、重复编号是哪个、温度/pH 各取哪条记录”。

    温度与 pH 的具体数值由服务端在事务内从两条当前已提交记录读取，
    避免客户端用可能过期的数值覆盖并发提交的新值。
    """

    keep_id: int = Field(gt=0)
    duplicate_id: int = Field(gt=0)
    temp_from: Literal["keep", "duplicate"]
    ph_from: Literal["keep", "duplicate"]
