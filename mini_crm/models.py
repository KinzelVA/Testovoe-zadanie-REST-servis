from datetime import datetime
import enum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from mini_crm.database import Base


class Operator(Base):
    __tablename__ = "operators"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    load_limit = Column(Integer, nullable=False, default=10)

    weights = relationship(
        "OperatorSourceWeight",
        back_populates="operator",
        cascade="all, delete-orphan",
    )

    contacts = relationship("Contact", back_populates="operator")


class Lead(Base):
    __tablename__ = "leads"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, nullable=False, unique=True, index=True)
    name = Column(String, nullable=True)

    contacts = relationship("Contact", back_populates="lead")


class Source(Base):
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True)
    code = Column(String, nullable=True, unique=True)

    weights = relationship(
        "OperatorSourceWeight",
        back_populates="source",
        cascade="all, delete-orphan",
    )
    contacts = relationship("Contact", back_populates="source")


class OperatorSourceWeight(Base):
    __tablename__ = "operator_source_weights"
    __table_args__ = (
        UniqueConstraint(
            "operator_id",
            "source_id",
            name="uix_operator_source",
        ),
    )

    id = Column(Integer, primary_key=True, index=True)
    operator_id = Column(
        Integer,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_id = Column(
        Integer,
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
    )
    weight = Column(Integer, nullable=False)

    operator = relationship("Operator", back_populates="weights")
    source = relationship("Source", back_populates="weights")


class ContactStatus(str, enum.Enum):
    active = "active"
    closed = "closed"


class Contact(Base):
    __tablename__ = "contacts"

    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, ForeignKey("leads.id"), nullable=False)
    source_id = Column(Integer, ForeignKey("sources.id"), nullable=False)
    operator_id = Column(Integer, ForeignKey("operators.id"), nullable=True)
    status = Column(
        Enum(ContactStatus),
        nullable=False,
        default=ContactStatus.active,
    )
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    payload = Column(String, nullable=True)

    lead = relationship("Lead", back_populates="contacts")
    source = relationship("Source", back_populates="contacts")
    operator = relationship("Operator", back_populates="contacts")
