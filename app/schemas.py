from pydantic import BaseModel, Field


class SampleIn(BaseModel):
    # 两值必填且非空（与旧表 NOT NULL 约束一致）
    temp: float = Field(allow_inf_nan=False)
    ph: float = Field(allow_inf_nan=False)


class SampleOut(BaseModel):
    id: int
    temp: float
    ph: float
