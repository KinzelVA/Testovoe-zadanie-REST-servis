from datetime import datetime
import random
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func
from sqlalchemy.orm import Session

from mini_crm import models
from mini_crm.database import SessionLocal, engine

# Создаём таблицы при старте (для тестового задания достаточно)
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Mini-CRM lead distribution")


# --- DB dependency ---


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- Pydantic схемы ---


class OperatorCreate(BaseModel):
    name: str
    load_limit: int = 10


class OperatorUpdate(BaseModel):
    load_limit: Optional[int] = None
    is_active: Optional[bool] = None


class OperatorOut(BaseModel):
    id: int
    name: str
    is_active: bool
    load_limit: int

    model_config = ConfigDict(from_attributes=True)


class SourceCreate(BaseModel):
    name: str
    code: Optional[str] = None


class SourceOut(BaseModel):
    id: int
    name: str
    code: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class WeightCreate(BaseModel):
    operator_id: int
    source_id: int
    weight: int


class WeightOut(BaseModel):
    id: int
    operator_id: int
    source_id: int
    weight: int

    model_config = ConfigDict(from_attributes=True)


class LeadOut(BaseModel):
    id: int
    external_id: str
    name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class ContactCreate(BaseModel):
    """Данные для регистрации обращения.

    lead_external_id — по нему мы однозначно идентифицируем лида.
    """

    lead_external_id: str
    lead_name: Optional[str] = None
    source_id: int
    payload: Optional[str] = None


class ContactShortOut(BaseModel):
    id: int
    source_id: int
    operator_id: Optional[int] = None
    status: models.ContactStatus
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class LeadWithContactsOut(BaseModel):
    id: int
    external_id: str
    name: Optional[str] = None
    contacts: List[ContactShortOut] = []

    model_config = ConfigDict(from_attributes=True)


class ContactOut(BaseModel):
    id: int
    status: models.ContactStatus
    created_at: datetime
    payload: Optional[str] = None
    lead: LeadOut
    source: SourceOut
    operator: Optional[OperatorOut] = None

    model_config = ConfigDict(from_attributes=True)


class ContactStatusUpdate(BaseModel):
    status: models.ContactStatus


class OperatorStatsOut(BaseModel):
    operator_id: int
    operator_name: str
    contacts_count: int


class SourceStatsOut(BaseModel):
    source_id: int
    source_name: str
    contacts_count: int


# --- Служебные функции ---


def get_or_create_lead(
    db: Session,
    external_id: str,
    name: Optional[str] = None,
) -> models.Lead:
    lead = (
        db.query(models.Lead)
        .filter(models.Lead.external_id == external_id)
        .first()
    )
    if lead:
        if name and not lead.name:
            lead.name = name
            db.commit()
            db.refresh(lead)
        return lead

    lead = models.Lead(external_id=external_id, name=name)
    db.add(lead)
    db.commit()
    db.refresh(lead)
    return lead


def choose_operator_for_source(db: Session, source_id: int) -> Optional[models.Operator]:
    """Выбор оператора по весам и лимиту нагрузки."""

    weights = (
        db.query(models.OperatorSourceWeight)
        .join(models.Operator)
        .filter(
            models.OperatorSourceWeight.source_id == source_id,
            models.Operator.is_active.is_(True),
        )
        .all()
    )

    candidates: list[tuple[models.Operator, int]] = []

    for w in weights:
        op = w.operator
        if w.weight <= 0:
            continue

        active_count = (
            db.query(models.Contact)
            .filter(
                models.Contact.operator_id == op.id,
                models.Contact.status == models.ContactStatus.active,
            )
            .count()
        )

        if active_count < op.load_limit:
            candidates.append((op, w.weight))

    if not candidates:
        return None

    total_weight = sum(weight for _, weight in candidates)
    r = random.uniform(0, total_weight)
    upto = 0.0
    for op, weight in candidates:
        if upto + weight >= r:
            return op
        upto += weight

    return candidates[-1][0]


# --- Операторы ---


@app.post("/operators", response_model=OperatorOut)
def create_operator(
    operator: OperatorCreate,
    db: Session = Depends(get_db),
):
    db_operator = models.Operator(
        name=operator.name,
        load_limit=operator.load_limit,
    )
    db.add(db_operator)
    db.commit()
    db.refresh(db_operator)
    return db_operator


@app.get("/operators", response_model=List[OperatorOut])
def list_operators(db: Session = Depends(get_db)):
    operators = db.query(models.Operator).all()
    return operators


@app.patch("/operators/{operator_id}", response_model=OperatorOut)
def update_operator(
    operator_id: int,
    update: OperatorUpdate,
    db: Session = Depends(get_db),
):
    operator = (
        db.query(models.Operator)
        .filter(models.Operator.id == operator_id)
        .first()
    )
    if not operator:
        raise HTTPException(status_code=404, detail="Operator not found")

    if update.load_limit is not None:
        operator.load_limit = update.load_limit
    if update.is_active is not None:
        operator.is_active = update.is_active

    db.commit()
    db.refresh(operator)
    return operator


# --- Источники и веса ---


@app.post("/sources", response_model=SourceOut)
def create_source(
    source: SourceCreate,
    db: Session = Depends(get_db),
):
    db_source = models.Source(name=source.name, code=source.code)
    db.add(db_source)
    db.commit()
    db.refresh(db_source)
    return db_source


@app.get("/sources", response_model=List[SourceOut])
def list_sources(db: Session = Depends(get_db)):
    sources = db.query(models.Source).all()
    return sources


@app.post("/weights", response_model=WeightOut)
def set_weight(
    weight_in: WeightCreate,
    db: Session = Depends(get_db),
):
    weight = (
        db.query(models.OperatorSourceWeight)
        .filter(
            models.OperatorSourceWeight.operator_id == weight_in.operator_id,
            models.OperatorSourceWeight.source_id == weight_in.source_id,
        )
        .first()
    )

    if weight is None:
        weight = models.OperatorSourceWeight(
            operator_id=weight_in.operator_id,
            source_id=weight_in.source_id,
            weight=weight_in.weight,
        )
        db.add(weight)
    else:
        weight.weight = weight_in.weight

    db.commit()
    db.refresh(weight)
    return weight


@app.get("/sources/{source_id}/weights", response_model=List[WeightOut])
def list_weights_for_source(
    source_id: int,
    db: Session = Depends(get_db),
):
    weights = (
        db.query(models.OperatorSourceWeight)
        .filter(models.OperatorSourceWeight.source_id == source_id)
        .all()
    )
    return weights


# --- Регистрация обращения ---


@app.post("/contacts", response_model=ContactOut)
def create_contact(
    contact_in: ContactCreate,
    db: Session = Depends(get_db),
):
    source = (
        db.query(models.Source)
        .filter(models.Source.id == contact_in.source_id)
        .first()
    )
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    lead = get_or_create_lead(
        db,
        external_id=contact_in.lead_external_id,
        name=contact_in.lead_name,
    )

    operator = choose_operator_for_source(db, source_id=source.id)

    db_contact = models.Contact(
        lead_id=lead.id,
        source_id=source.id,
        operator_id=operator.id if operator else None,
        status=models.ContactStatus.active,
        payload=contact_in.payload,
    )
    db.add(db_contact)
    db.commit()
    db.refresh(db_contact)
    return db_contact


@app.patch("/contacts/{contact_id}", response_model=ContactOut)
def update_contact_status(
    contact_id: int,
    update: ContactStatusUpdate,
    db: Session = Depends(get_db),
):
    contact = (
        db.query(models.Contact)
        .filter(models.Contact.id == contact_id)
        .first()
    )
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    contact.status = update.status
    db.commit()
    db.refresh(contact)
    return contact


# --- Просмотр лидов и статистики ---


@app.get("/leads", response_model=List[LeadWithContactsOut])
def list_leads(db: Session = Depends(get_db)):
    leads = db.query(models.Lead).all()
    return leads


@app.get("/stats/operators", response_model=List[OperatorStatsOut])
def stats_by_operator(db: Session = Depends(get_db)):
    rows = (
        db.query(
            models.Operator.id.label("operator_id"),
            models.Operator.name.label("operator_name"),
            func.count(models.Contact.id).label("contacts_count"),
        )
        .outerjoin(
            models.Contact,
            models.Contact.operator_id == models.Operator.id,
        )
        .group_by(models.Operator.id)
        .all()
    )

    return [
        OperatorStatsOut(
            operator_id=row.operator_id,
            operator_name=row.operator_name,
            contacts_count=row.contacts_count,
        )
        for row in rows
    ]


@app.get("/stats/sources", response_model=List[SourceStatsOut])
def stats_by_source(db: Session = Depends(get_db)):
    rows = (
        db.query(
            models.Source.id.label("source_id"),
            models.Source.name.label("source_name"),
            func.count(models.Contact.id).label("contacts_count"),
        )
        .outerjoin(
            models.Contact,
            models.Contact.source_id == models.Source.id,
        )
        .group_by(models.Source.id)
        .all()
    )

    return [
        SourceStatsOut(
            source_id=row.source_id,
            source_name=row.source_name,
            contacts_count=row.contacts_count,
        )
        for row in rows
    ]
