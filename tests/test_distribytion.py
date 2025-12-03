import os
import sys

# Добавляем в sys.path корень проекта (папку, где лежит mini_crm)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pytest

from mini_crm.database import Base, engine, SessionLocal
from mini_crm import models
from mini_crm.main import choose_operator_for_source


@pytest.fixture(autouse=True)
def setup_db():
    """
    Перед каждым тестом пересоздаём таблицы,
    чтобы тесты были изолированы.
    """
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


def create_session():
    return SessionLocal()


def test_choose_operator_respects_weights(monkeypatch):
    """
    Проверяем, что при разных весах операторов и фиксированном random.uniform
    выбирается ожидаемый оператор.
    """
    db = create_session()

    op1 = models.Operator(name="Op1", load_limit=10)
    op2 = models.Operator(name="Op2", load_limit=10)
    db.add_all([op1, op2])
    db.commit()
    db.refresh(op1)
    db.refresh(op2)

    source = models.Source(name="Bot A", code="bot_a")
    db.add(source)
    db.commit()
    db.refresh(source)

    w1 = models.OperatorSourceWeight(operator_id=op1.id, source_id=source.id, weight=1)
    w2 = models.OperatorSourceWeight(operator_id=op2.id, source_id=source.id, weight=3)
    db.add_all([w1, w2])
    db.commit()

    # total_weight = 4, r = 2.5 => должен выбраться op2
    monkeypatch.setattr("mini_crm.main.random.uniform", lambda a, b: 2.5)

    chosen = choose_operator_for_source(db, source_id=source.id)

    assert chosen is not None
    assert chosen.id == op2.id


def test_choose_operator_respects_load_limit():
    """
    Проверяем, что оператор с выбранным лимитом не попадает в кандидаты.
    """
    db = create_session()

    op = models.Operator(name="Limited Op", load_limit=1)
    db.add(op)
    db.commit()
    db.refresh(op)

    source = models.Source(name="Bot B", code="bot_b")
    db.add(source)
    db.commit()
    db.refresh(source)

    weight = models.OperatorSourceWeight(operator_id=op.id, source_id=source.id, weight=10)
    db.add(weight)
    db.commit()

    lead = models.Lead(external_id="user_1", name="User 1")
    db.add(lead)
    db.commit()
    db.refresh(lead)

    contact = models.Contact(
        lead_id=lead.id,
        source_id=source.id,
        operator_id=op.id,
        status=models.ContactStatus.active,
        payload="Первое обращение",
    )
    db.add(contact)
    db.commit()

    chosen = choose_operator_for_source(db, source_id=source.id)

    assert chosen is None
