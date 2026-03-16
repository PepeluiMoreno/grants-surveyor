"""
models/odm.py — Modelos de integración con OpenDataManager.

Representan los objetos de ODM tal como los ve GrantsSurveyor:
  - ODMDataset:      un Dataset publicado por ODM
  - ODMWebhookEvent: el payload que ODM envía al webhook
  - ODMSubscription: la suscripción de esta investigación a un Resource

Estos modelos son de solo lectura desde el punto de vista de GrantsSurveyor.
ODM es la fuente de verdad; GrantsSurveyor solo consume.

Schema de referencia (migración d47f88170fe1 de OpenDataManager):
  opendata.dataset          — versionado semántico de datos
  opendata.dataset_subscription — qué versiones acepta cada Application
  opendata.application_notification — log de webhooks enviados
  opendata.resource         — fuente de datos configurada
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from .base import UpgradePolicy


class ODMSchemaField(BaseModel):
    """Un campo garantizado en el schema de un Dataset de ODM."""
    name: str
    data_type: str          # "string" | "integer" | "decimal" | "date" | "boolean"
    nullable: bool = True
    description: str = ""


class ODMDataset(BaseModel):
    """
    Representa un Dataset publicado por OpenDataManager.
    Contiene el schema de campos garantizados y metadatos de versión.
    """
    dataset_id: str                         # UUID del Dataset en ODM
    resource_id: str                        # UUID del Resource en ODM
    resource_code: str                      # ej: "bdns_concesiones"
    resource_name: str

    # Versión semántica
    major_version: int
    minor_version: int
    patch_version: int

    # Schema de campos garantizados (del campo schema_json de ODM)
    schema_fields: list[ODMSchemaField] = Field(default_factory=list)

    # Metadatos
    record_count: int | None = None
    checksum: str | None = None
    data_path: str | None = None            # ruta en el filesystem de ODM
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def version_str(self) -> str:
        return f"{self.major_version}.{self.minor_version}.{self.patch_version}"

    @property
    def field_names(self) -> list[str]:
        return [f.name for f in self.schema_fields]

    def is_compatible_upgrade_from(self, other_version: str) -> bool:
        """
        True si este dataset es un upgrade compatible desde other_version.
        Compatible = mismo major o minor upgrade.
        Incompatible = major upgrade (requiere revisión humana).
        """
        parts = other_version.split(".")
        if len(parts) != 3:
            return False
        other_major = int(parts[0])
        return self.major_version == other_major


class ODMWebhookEvent(BaseModel):
    """
    Payload del webhook que OpenDataManager envía a GrantsSurveyor
    cuando hay un Dataset nuevo disponible.

    ODM firma el payload con HMAC-SHA256 usando webhook_secret.
    El WebhookReceiver verifica la firma antes de procesar.
    """
    event: str                              # "dataset.new_version"
    resource_id: str
    resource_code: str                      # ej: "bdns_concesiones"
    resource_name: str
    dataset_id: str
    version: str                            # "1.4.0"
    prev_version: str | None = None         # versión anterior que tenía la suscripción
    record_count: int | None = None
    schema_json: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    odm_api_url: str = ""                   # URL base del GraphQL de ODM

    @property
    def is_major_upgrade(self) -> bool:
        if not self.prev_version:
            return False
        prev_major = int(self.prev_version.split(".")[0])
        curr_major = int(self.version.split(".")[0])
        return curr_major > prev_major

    def to_odm_dataset(self) -> ODMDataset:
        """Convierte el evento en un ODMDataset parseable."""
        parts = self.version.split(".")
        fields = [
            ODMSchemaField(name=k, data_type=v if isinstance(v, str) else "string")
            for k, v in self.schema_json.items()
        ]
        return ODMDataset(
            dataset_id=self.dataset_id,
            resource_id=self.resource_id,
            resource_code=self.resource_code,
            resource_name=self.resource_name,
            major_version=int(parts[0]),
            minor_version=int(parts[1]),
            patch_version=int(parts[2]),
            schema_fields=fields,
            record_count=self.record_count,
        )


class ODMSubscription(BaseModel):
    """
    Suscripción de una investigación de GrantsSurveyor a un Resource de ODM.

    Controla qué versiones acepta automáticamente y
    qué versión tiene actualmente procesada.

    Mapeada a opendata.dataset_subscription en ODM.
    """
    investigation_id: str
    resource_id: str
    resource_code: str
    resource_name: str

    auto_upgrade: UpgradePolicy = UpgradePolicy.MINOR
    pinned_version: str | None = None       # None = sigue la última
    current_version: str | None = None      # última versión procesada
    notified_at: datetime | None = None

    active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)

    def should_process(self, event: ODMWebhookEvent) -> bool:
        """
        ¿Debe esta suscripción procesar el evento recibido?

        - Si la versión está fijada (pinned), solo procesa esa versión.
        - Si es major upgrade y la política es MINOR o PATCH, no procesa.
        - En cualquier otro caso, procesa.
        """
        if self.pinned_version and event.version != self.pinned_version:
            return False
        if event.is_major_upgrade:
            return self.auto_upgrade == UpgradePolicy.MAJOR
        return True

    def requires_human_review_for(self, event: ODMWebhookEvent) -> bool:
        """True si el cambio de versión requiere revisión humana antes de procesar."""
        return event.is_major_upgrade and self.auto_upgrade != UpgradePolicy.MAJOR
