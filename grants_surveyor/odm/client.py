"""
odm/client.py — Cliente async para la API GraphQL de OpenDataManager.

Responsabilidades:
  - Consultar datasets disponibles y sus schemas
  - Descargar registros de un dataset por resource_code y versión
  - Registrar GrantsSurveyor como Application en ODM
  - Gestionar DatasetSubscriptions

Configuración vía variables de entorno (.env):
  ODM_API_HOST=optiplex-790        # hostname o IP de OpenDataManager
  ODM_API_PORT=8040                # puerto (por defecto 8040)
  ODM_API_SCHEME=http              # http o https
  ODM_GRAPHQL_PATH=/graphql        # path del endpoint GraphQL

La URL final se construye como:
  {ODM_API_SCHEME}://{ODM_API_HOST}:{ODM_API_PORT}{ODM_GRAPHQL_PATH}

También se puede sobreescribir con la URL completa:
  ODM_GRAPHQL_URL=http://optiplex-790:8040/graphql
  (si está definida, tiene prioridad sobre las variables individuales)
"""
from __future__ import annotations

import logging
import os
from typing import Any, AsyncIterator

import httpx

from ..models import ODMDataset, ODMSchemaField, ODMSubscription, UpgradePolicy


def _odm_graphql_url() -> str:
    """
    Construye la URL del endpoint GraphQL de ODM desde variables de entorno.
    ODM_GRAPHQL_URL tiene prioridad. Si no está definida, construye desde partes.
    """
    full_url = os.getenv("ODM_GRAPHQL_URL")
    if full_url:
        return full_url
    scheme = os.getenv("ODM_API_SCHEME", "http")
    host   = os.getenv("ODM_API_HOST", "localhost")
    port   = os.getenv("ODM_API_PORT", "8040")
    path   = os.getenv("ODM_GRAPHQL_PATH", "/graphql")
    return f"{scheme}://{host}:{port}{path}"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Queries y mutations GraphQL
# ---------------------------------------------------------------------------

_Q_RESOURCES = """
query ListResources($activeOnly: Boolean) {
  resources(activeOnly: $activeOnly) {
    id
    name
    description
    publisher
    targetTable
    active
    fetcher {
      id
      name
      classPath
    }
    params {
      key
      value
    }
  }
}
"""

_Q_DATASET_BY_RESOURCE = """
query LatestDataset($resourceId: String!) {
  datasets(resourceId: $resourceId, limit: 1) {
    id
    resourceId
    majorVersion
    minorVersion
    patchVersion
    schemaJson
    recordCount
    checksum
    dataPath
    createdAt
    resource {
      name
    }
  }
}
"""

_Q_DATASET_RECORDS = """
query DatasetRecords($datasetId: String!, $offset: Int!, $limit: Int!) {
  datasetRecords(datasetId: $datasetId, offset: $offset, limit: $limit) {
    records
    total
    hasMore
  }
}
"""

_Q_APPLICATION = """
query GetApplication($name: String!) {
  applications(name: $name) {
    id
    name
    webhookUrl
    active
    subscribedResources
  }
}
"""

_M_CREATE_APPLICATION = """
mutation CreateApplication($input: CreateApplicationInput!) {
  createApplication(input: $input) {
    id
    name
    webhookUrl
    active
  }
}
"""

_M_EXECUTE_RESOURCE = """
mutation ExecuteResource($id: String!) {
  executeResource(id: $id) {
    success
    message
    executionId
  }
}
"""

_Q_RESOURCE_EXECUTIONS = """
query ResourceExecutions($resourceId: String!) {
  resourceExecutions(resourceId: $resourceId) {
    id
    status
    totalRecords
    recordsLoaded
    startedAt
    completedAt
    stagingPath
    errorMessage
  }
}
"""


# ---------------------------------------------------------------------------
# Cliente
# ---------------------------------------------------------------------------

class ODMClient:
    """
    Cliente async para la API GraphQL de OpenDataManager.

    Si no se pasa graphql_url, la construye desde variables de entorno:
      ODM_GRAPHQL_URL  (prioridad, URL completa)
      ODM_API_SCHEME + ODM_API_HOST + ODM_API_PORT + ODM_GRAPHQL_PATH

    Uso sin hardcodear nada:
        async with ODMClient() as client:          # lee de .env
            resources = await client.list_resources()

    Uso con URL explícita (tests, CI):
        async with ODMClient("http://host:8040/graphql") as client:
            dataset = await client.get_latest_dataset("bdns_concesiones")
    """

    def __init__(
        self,
        graphql_url: str | None = None,
        timeout: float = 30.0,
        page_size: int = 500,
    ) -> None:
        self.url = graphql_url or _odm_graphql_url()
        self.timeout = timeout
        self.page_size = page_size
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "ODMClient":
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={"Content-Type": "application/json"},
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Usa ODMClient como async context manager")
        return self._client

    # ------------------------------------------------------------------
    # GraphQL base
    # ------------------------------------------------------------------

    async def _query(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Ejecuta una query o mutation GraphQL y devuelve data."""
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            response = await self.client.post(self.url, json=payload)
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Error HTTP en ODM GraphQL: %s", e)
            raise

        result = response.json()

        if "errors" in result:
            errors = result["errors"]
            logger.error("Errores GraphQL de ODM: %s", errors)
            raise ODMClientError(f"GraphQL errors: {errors}")

        return result.get("data", {})

    # ------------------------------------------------------------------
    # Resources
    # ------------------------------------------------------------------

    async def list_resources(self, active_only: bool = True) -> list[dict[str, Any]]:
        """Lista todos los Resources disponibles en OpenDataManager."""
        data = await self._query(_Q_RESOURCES, {"activeOnly": active_only})
        return data.get("resources", [])

    async def get_resource_by_code(self, resource_code: str) -> dict[str, Any] | None:
        """Busca un Resource por su nombre/código."""
        resources = await self.list_resources(active_only=False)
        return next(
            (r for r in resources if r["name"] == resource_code),
            None
        )

    # ------------------------------------------------------------------
    # Datasets
    # ------------------------------------------------------------------

    async def get_latest_dataset(self, resource_id: str) -> ODMDataset | None:
        """
        Obtiene el Dataset más reciente de un Resource.
        Devuelve None si no hay ningún dataset disponible.
        """
        data = await self._query(_Q_DATASET_BY_RESOURCE, {"resourceId": resource_id})
        datasets = data.get("datasets", [])
        if not datasets:
            return None
        return self._parse_dataset(datasets[0])

    async def get_dataset_schema(self, resource_id: str) -> list[ODMSchemaField]:
        """Devuelve los campos garantizados del último dataset de un Resource."""
        dataset = await self.get_latest_dataset(resource_id)
        if not dataset:
            return []
        return dataset.schema_fields

    # ------------------------------------------------------------------
    # Registros
    # ------------------------------------------------------------------

    async def iter_records(
        self,
        dataset_id: str,
        page_size: int | None = None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Itera todos los registros de un Dataset en lotes.

        Yields listas de registros (dicts con los campos del schema_json).
        """
        size = page_size or self.page_size
        offset = 0
        total_fetched = 0

        while True:
            data = await self._query(
                _Q_DATASET_RECORDS,
                {"datasetId": dataset_id, "offset": offset, "limit": size},
            )
            result = data.get("datasetRecords", {})
            records: list[dict[str, Any]] = result.get("records", [])
            has_more: bool = result.get("hasMore", False)
            total: int = result.get("total", 0)

            if not records:
                break

            yield records
            total_fetched += len(records)

            logger.debug(
                "ODM records: %d/%d (dataset=%s)",
                total_fetched, total, dataset_id
            )

            if not has_more:
                break
            offset += size

    async def get_all_records(
        self, dataset_id: str
    ) -> list[dict[str, Any]]:
        """Descarga todos los registros de un Dataset en memoria."""
        all_records: list[dict[str, Any]] = []
        async for batch in self.iter_records(dataset_id):
            all_records.extend(batch)
        return all_records

    # ------------------------------------------------------------------
    # Application registration
    # ------------------------------------------------------------------

    async def get_or_create_application(
        self,
        name: str,
        webhook_url: str,
        webhook_secret: str,
        models_path: str = "grants_surveyor.odm_models",
    ) -> dict[str, Any]:
        """
        Obtiene la Application de GrantsSurveyor en ODM o la crea si no existe.
        """
        data = await self._query(_Q_APPLICATION, {"name": name})
        applications = data.get("applications", [])
        if applications:
            logger.info("Application '%s' ya existe en ODM.", name)
            return applications[0]

        # Crear nueva Application
        data = await self._query(_M_CREATE_APPLICATION, {
            "input": {
                "name": name,
                "webhookUrl": webhook_url,
                "webhookSecret": webhook_secret,
                "modelsPath": models_path,
                "active": True,
            }
        })
        app = data.get("createApplication", {})
        logger.info("Application '%s' creada en ODM (id=%s).", name, app.get("id"))
        return app

    # ------------------------------------------------------------------
    # Executions
    # ------------------------------------------------------------------

    async def trigger_resource_execution(self, resource_id: str) -> dict[str, Any]:
        """Lanza la ejecución de un Resource manualmente."""
        data = await self._query(_M_EXECUTE_RESOURCE, {"id": resource_id})
        result = data.get("executeResource", {})
        if not result.get("success"):
            raise ODMClientError(
                f"Error ejecutando resource {resource_id}: {result.get('message')}"
            )
        logger.info(
            "Resource %s ejecutado. execution_id=%s",
            resource_id, result.get("executionId")
        )
        return result

    async def get_resource_executions(
        self, resource_id: str
    ) -> list[dict[str, Any]]:
        """Historial de ejecuciones de un Resource."""
        data = await self._query(_Q_RESOURCE_EXECUTIONS, {"resourceId": resource_id})
        return data.get("resourceExecutions", [])

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_dataset(raw: dict[str, Any]) -> ODMDataset:
        """Convierte un dict GraphQL en ODMDataset."""
        schema_json: dict[str, Any] = raw.get("schemaJson") or {}

        # schema_json puede ser {field_name: type_str} o lista de objetos
        if isinstance(schema_json, dict):
            fields = [
                ODMSchemaField(name=k, data_type=str(v))
                for k, v in schema_json.items()
            ]
        elif isinstance(schema_json, list):
            fields = [
                ODMSchemaField(
                    name=f.get("name", ""),
                    data_type=f.get("dataType", f.get("data_type", "string")),
                    nullable=f.get("nullable", True),
                    description=f.get("description", ""),
                )
                for f in schema_json
            ]
        else:
            fields = []

        resource_name = (
            raw.get("resource", {}).get("name", "")
            if isinstance(raw.get("resource"), dict)
            else raw.get("resourceId", "")
        )

        return ODMDataset(
            dataset_id=raw["id"],
            resource_id=raw["resourceId"],
            resource_code=resource_name,
            resource_name=resource_name,
            major_version=raw.get("majorVersion", 1),
            minor_version=raw.get("minorVersion", 0),
            patch_version=raw.get("patchVersion", 0),
            schema_fields=fields,
            record_count=raw.get("recordCount"),
            checksum=raw.get("checksum"),
            data_path=raw.get("dataPath"),
        )


# ---------------------------------------------------------------------------
# Normalizer — mapea registros ODM al metadominio de GrantsSurveyor
# ---------------------------------------------------------------------------

class ODMNormalizer:
    """
    Mapea un registro crudo de ODM (dict con campos del schema_json)
    a los atributos del metadominio de GrantsSurveyor.

    Cada Investigation define su propio mapeo via resource_field_map,
    que se guarda en el WatchProfile. Si no hay mapeo explícito,
    se usan todos los campos del registro como Attributes dinámicos.

    Uso:
        normalizer = ODMNormalizer(
            resource_code="bdns_concesiones",
            field_map={
                "nif_beneficiario": "nif",
                "beneficiario":     "nombre",
                "importe":          "amount",
                "fecha_concesion":  "date",
                "organo_concedente":"granting_body",
            }
        )
        attrs, identifiers = normalizer.normalize(record, source_id, dataset_version)
    """

    def __init__(
        self,
        resource_code: str,
        field_map: dict[str, str] | None = None,
        identifier_fields: list[str] | None = None,
    ) -> None:
        self.resource_code = resource_code
        # field_map: {campo_odm → clave_atributo}
        self.field_map = field_map or {}
        # identifier_fields: campos que van a node.identifiers (búsqueda rápida)
        self.identifier_fields = identifier_fields or []

    def normalize(
        self,
        record: dict[str, Any],
        source_id: str,
        dataset_version: str,
        confidence: float = 1.0,
    ) -> tuple[list[dict[str, Any]], dict[str, str]]:
        """
        Normaliza un registro ODM.

        Devuelve:
          - lista de dicts para construir Attribute objects
          - dict de identifiers {key: value} para node.identifiers
        """
        from datetime import datetime

        attributes: list[dict[str, Any]] = []
        identifiers: dict[str, str] = {}

        for odm_key, value in record.items():
            if value is None or value == "":
                continue

            # Mapea el nombre del campo
            attr_key = self.field_map.get(odm_key, odm_key)

            # Limpia el valor
            clean_value = self._clean_value(value)

            attributes.append({
                "key": attr_key,
                "value": clean_value,
                "source_id": source_id,
                "confidence": confidence,
                "odm_dataset_version": dataset_version,
                "observed_at": datetime.utcnow(),
            })

            # Si es campo identificador, añade también a identifiers
            if odm_key in self.identifier_fields or attr_key in self.identifier_fields:
                if isinstance(clean_value, str) and clean_value:
                    identifiers[attr_key] = clean_value

        return attributes, identifiers

    @staticmethod
    def _clean_value(value: Any) -> Any:
        """Limpieza básica de valores."""
        if isinstance(value, str):
            return value.strip() or None
        return value

    @classmethod
    def for_bdns_concesiones(cls) -> "ODMNormalizer":
        """Normalizer preconfigurado para el resource bdns_concesiones."""
        return cls(
            resource_code="bdns_concesiones",
            field_map={
                "id_bdns":           "bdns_id",
                "nif_beneficiario":  "nif",
                "beneficiario":      "nombre",
                "importe":           "amount",
                "fecha_concesion":   "date",
                "organo_concedente": "granting_body",
                "cod_ccaa":          "ccaa_code",
                "desc_convocatoria": "call_name",
            },
            identifier_fields=["nif", "bdns_id"],
        )

    @classmethod
    def for_rer_entidades(cls) -> "ODMNormalizer":
        """Normalizer preconfigurado para el resource rer_entidades."""
        return cls(
            resource_code="rer_entidades",
            field_map={
                "id_rer":          "rer_id",
                "nombre":          "nombre",
                "tipo_entidad":    "actor_type",
                "estado":          "rer_status",
                "fecha_inscripcion": "registration_date",
            },
            identifier_fields=["rer_id", "nombre"],
        )

    @classmethod
    def from_watch_profile_config(
        cls, resource_code: str, config: dict[str, Any]
    ) -> "ODMNormalizer":
        """
        Crea un normalizer desde la configuración del WatchProfile.
        Permite al analista definir mapeos personalizados.
        """
        return cls(
            resource_code=resource_code,
            field_map=config.get("field_map", {}),
            identifier_fields=config.get("identifier_fields", []),
        )


# ---------------------------------------------------------------------------
# Excepciones
# ---------------------------------------------------------------------------

class ODMClientError(Exception):
    """Error en la comunicación con OpenDataManager."""
    pass


class ODMSchemaChangedError(ODMClientError):
    """
    El schema del dataset cambió de forma incompatible (major version).
    Requiere revisión humana antes de continuar.
    """
    def __init__(self, resource_code: str, old_version: str, new_version: str) -> None:
        self.resource_code = resource_code
        self.old_version = old_version
        self.new_version = new_version
        super().__init__(
            f"Schema incompatible en '{resource_code}': "
            f"{old_version} → {new_version}. "
            f"Revisa el nuevo contrato de campos antes de continuar."
        )
