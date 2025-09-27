"""
Semantic-Aware Data Collectors for Mississippi River Navigation System
Implements ET(K)L pattern with knowledge extraction during data acquisition

Architecture:
1. Connect to data source
2. Structure data if not yet structured  
3. Apply semantic metadata extraction (using KuzuDB as temporary graph)
4. Store semantically-enriched data ready for graph analytics
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
import logging

import httpx
import pandas as pd
import kuzu
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL, GEO
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import numpy as np

# Domain-specific namespaces
NAV = Namespace("http://mississippi.navigation.org/ontology/")
HYDRO = Namespace("http://hydrology.usgs.gov/ontology/")
TRANSPORT = Namespace("http://transportation.dot.gov/ontology/")
COMMODITY = Namespace("http://commodities.usda.gov/ontology/")
GEONAMES = Namespace("http://www.geonames.org/ontology#")

logger = logging.getLogger(__name__)


@dataclass
class SemanticContext:
    """Semantic context for data source understanding"""
    domain: str
    ontology_uri: str
    primary_concepts: List[str]
    entity_types: List[str]
    spatial_context: Optional[str] = None
    temporal_context: Optional[str] = None
    unit_mappings: Dict[str, str] = None


@dataclass
class StructuredRecord:
    """Semantically-enriched data record"""
    source_id: str
    record_id: str
    timestamp: datetime
    raw_data: Dict[str, Any]
    structured_data: Dict[str, Any]
    semantic_annotations: Dict[str, Any]
    spatial_info: Optional[Dict[str, float]] = None
    temporal_info: Optional[Dict[str, datetime]] = None
    quality_metrics: Optional[Dict[str, float]] = None


class SemanticCollectorBase(ABC):
    """
    Base class for semantically-aware data collectors
    Each collector implements ET(K)L with built-in knowledge extraction
    """
    
    def __init__(self, source_name: str, kuzu_temp_db: str, semantic_context: SemanticContext):
        self.source_name = source_name
        self.semantic_context = semantic_context
        
        # Temporary KuzuDB for semantic processing during acquisition
        self.temp_db = kuzu.Database(kuzu_temp_db)
        self.temp_conn = kuzu.Connection(self.temp_db)
        
        # Initialize semantic processing tables
        self._setup_semantic_processing_schema()
        
        # In-memory semantic mappings cache
        self.entity_cache = {}
        self.concept_mappings = {}
        
        self._initialize_domain_knowledge()
    
    def _setup_semantic_processing_schema(self):
        """Setup temporary KuzuDB schema for semantic processing during acquisition"""
        
        # Temporary entity resolution table
        self.temp_conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS TempEntity(
                entity_id STRING,
                entity_type STRING,
                source_name STRING,
                raw_value STRING,
                canonical_form STRING,
                confidence_score DOUBLE,
                semantic_uri STRING,
                spatial_lat DOUBLE,
                spatial_lon DOUBLE,
                temporal_valid_from TIMESTAMP,
                temporal_valid_to TIMESTAMP,
                PRIMARY KEY (entity_id)
            )
        """)
        
        # Temporary concept mapping table
        self.temp_conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS TempConcept(
                concept_id STRING,
                raw_field_name STRING,
                canonical_concept STRING,
                ontology_mapping STRING,
                unit_of_measure STRING,
                data_type STRING,
                validation_rules STRING,
                PRIMARY KEY (concept_id)
            )
        """)
        
        # Semantic relationships table
        self.temp_conn.execute("""
            CREATE REL TABLE IF NOT EXISTS SEMANTIC_RELATION(
                FROM TempEntity TO TempEntity,
                relation_type STRING,
                confidence DOUBLE,
                source_evidence STRING
            )
        """)
    
    @abstractmethod
    def _initialize_domain_knowledge(self):
        """Initialize domain-specific knowledge for semantic processing"""
        pass
    
    @abstractmethod
    async def extract_raw_data(self) -> List[Dict[str, Any]]:
        """Extract raw data from source (E in ET(K)L)"""
        pass
    
    @abstractmethod
    def structure_data(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Structure raw data into consistent format (T in ET(K)L)"""
        pass
    
    def apply_semantic_enrichment(self, structured_record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply semantic metadata extraction (K in ET(K)L)"""
        
        semantic_annotations = {
            'entities': self._extract_entities(structured_record),
            'concepts': self._map_concepts(structured_record),
            'spatial_context': self._extract_spatial_context(structured_record),
            'temporal_context': self._extract_temporal_context(structured_record),
            'domain_classifications': self._classify_domain_concepts(structured_record),
            'quality_assessment': self._assess_data_quality(structured_record),
            'interoperability_mappings': self._create_interop_mappings(structured_record)
        }
        
        return semantic_annotations
    
    def _extract_entities(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract and resolve entities with semantic URIs"""
        
        entities = []
        
        for field_name, value in record.items():
            if value is None or value == '':
                continue
                
            # Entity recognition based on field patterns and domain knowledge
            entity_type = self._classify_entity_type(field_name, value)
            
            if entity_type:
                canonical_form = self._canonicalize_entity(entity_type, value)
                semantic_uri = self._resolve_semantic_uri(entity_type, canonical_form)
                confidence = self._calculate_entity_confidence(field_name, value, entity_type)
                
                entity = {
                    'field_name': field_name,
                    'raw_value': str(value),
                    'entity_type': entity_type,
                    'canonical_form': canonical_form,
                    'semantic_uri': semantic_uri,
                    'confidence_score': confidence
                }
                
                # Add spatial/temporal context if applicable
                if entity_type in ['location', 'facility', 'waterway']:
                    spatial_info = self._resolve_spatial_coordinates(canonical_form)
                    if spatial_info:
                        entity.update(spatial_info)
                
                entities.append(entity)
                
                # Store in temporary graph for cross-record entity resolution
                self._store_temp_entity(entity)
        
        return entities
    
    def _classify_entity_type(self, field_name: str, value: Any) -> Optional[str]:
        """Classify the type of entity based on field name and value patterns"""
        
        field_lower = field_name.lower()
        value_str = str(value).lower() if value else ""
        
        # Domain-specific entity classification
        if self.semantic_context.domain == "hydrology":
            if 'site' in field_lower or 'station' in field_lower:
                return 'gauge_station'
            elif 'river' in field_lower or 'waterway' in field_lower:
                return 'waterway'
            elif 'lock' in field_lower or 'dam' in field_lower:
                return 'infrastructure'
                
        elif self.semantic_context.domain == "transportation":
            if 'vessel' in field_lower or 'ship' in field_lower:
                return 'vessel'
            elif 'port' in field_lower or 'terminal' in field_lower:
                return 'facility'
            elif 'cargo' in field_lower or 'commodity' in field_lower:
                return 'commodity'
                
        elif self.semantic_context.domain == "economics":
            if 'commodity' in field_lower or 'grain' in field_lower:
                return 'commodity'
            elif 'market' in field_lower or 'exchange' in field_lower:
                return 'market'
            elif 'location' in field_lower or 'terminal' in field_lower:
                return 'location'
        
        # Generic patterns
        if 'lat' in field_lower or 'lon' in field_lower:
            return 'coordinate'
        elif 'time' in field_lower or 'date' in field_lower:
            return 'temporal'
        elif 'name' in field_lower and len(value_str) > 3:
            return 'named_entity'
            
        return None
    
    def _canonicalize_entity(self, entity_type: str, value: Any) -> str:
        """Convert entity to canonical form for consistent referencing"""
        
        value_str = str(value).strip()
        
        if entity_type == 'gauge_station':
            # USGS site codes are already canonical
            return value_str.upper()
            
        elif entity_type == 'waterway':
            # Standardize waterway names
            waterway_mappings = {
                'mississippi river': 'Mississippi River',
                'missouri river': 'Missouri River',
                'ohio river': 'Ohio River',
                'illinois waterway': 'Illinois Waterway'
            }
            return waterway_mappings.get(value_str.lower(), value_str.title())
            
        elif entity_type == 'vessel':
            # Clean vessel names
            return ' '.join(value_str.upper().split())
            
        elif entity_type == 'commodity':
            # Standardize commodity names
            commodity_mappings = {
                'corn': 'Corn',
                'soybeans': 'Soybeans',
                'wheat': 'Wheat',
                'coal': 'Coal',
                'petroleum': 'Petroleum Products'
            }
            return commodity_mappings.get(value_str.lower(), value_str.title())
            
        return value_str
    
    def _resolve_semantic_uri(self, entity_type: str, canonical_form: str) -> str:
        """Generate semantic URI for entity"""
        
        base_uris = {
            'gauge_station': 'http://hydrology.usgs.gov/site/',
            'waterway': 'http://mississippi.navigation.org/waterway/',
            'vessel': 'http://transportation.dot.gov/vessel/',
            'facility': 'http://transportation.dot.gov/facility/',
            'commodity': 'http://commodities.usda.gov/commodity/',
            'location': 'http://geonames.org/search?q='
        }
        
        base_uri = base_uris.get(entity_type, 'http://mississippi.navigation.org/entity/')
        canonical_id = canonical_form.lower().replace(' ', '_').replace('/', '_')
        
        return f"{base_uri}{canonical_id}"
    
    def _calculate_entity_confidence(self, field_name: str, value: Any, entity_type: str) -> float:
        """Calculate confidence score for entity extraction"""
        
        confidence = 0.5  # Base confidence
        
        # Boost confidence based on field name alignment
        if entity_type in field_name.lower():
            confidence += 0.2
            
        # Boost confidence based on value patterns
        value_str = str(value)
        if entity_type == 'gauge_station' and len(value_str) == 8 and value_str.isdigit():
            confidence += 0.3
        elif entity_type == 'vessel' and len(value_str) > 3:
            confidence += 0.2
        elif entity_type == 'commodity' and value_str.lower() in ['corn', 'soybeans', 'wheat']:
            confidence += 0.3
            
        return min(1.0, confidence)
    
    def _map_concepts(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Map fields to ontology concepts"""
        
        concept_mappings = []
        
        for field_name, value in record.items():
            ontology_mapping = self._find_ontology_mapping(field_name, value)
            
            if ontology_mapping:
                concept_mapping = {
                    'field_name': field_name,
                    'ontology_concept': ontology_mapping['concept'],
                    'ontology_uri': ontology_mapping['uri'],
                    'data_type': ontology_mapping['data_type'],
                    'unit_of_measure': ontology_mapping.get('unit'),
                    'validation_rules': ontology_mapping.get('validation', [])
                }
                
                concept_mappings.append(concept_mapping)
                
                # Store in temp graph for consistency across records
                self._store_temp_concept(concept_mapping)
        
        return concept_mappings
    
    def _find_ontology_mapping(self, field_name: str, value: Any) -> Optional[Dict[str, Any]]:
        """Find appropriate ontology mapping for field"""
        
        field_lower = field_name.lower()
        
        # Hydrological mappings
        hydro_mappings = {
            'water_level': {
                'concept': 'WaterLevel',
                'uri': str(HYDRO.WaterLevel),
                'data_type': 'float',
                'unit': 'feet',
                'validation': ['range(0, 100)']
            },
            'flow_rate': {
                'concept': 'FlowRate', 
                'uri': str(HYDRO.FlowRate),
                'data_type': 'float',
                'unit': 'cubic_feet_per_second',
                'validation': ['range(0, 1000000)']
            },
            'stage': {
                'concept': 'WaterLevel',
                'uri': str(HYDRO.WaterLevel),
                'data_type': 'float',
                'unit': 'feet'
            }
        }
        
        # Transportation mappings
        transport_mappings = {
            'vessel_name': {
                'concept': 'VesselIdentifier',
                'uri': str(TRANSPORT.vesselName),
                'data_type': 'string'
            },
            'speed': {
                'concept': 'VesselSpeed',
                'uri': str(TRANSPORT.speed),
                'data_type': 'float',
                'unit': 'knots',
                'validation': ['range(0, 50)']
            },
            'latitude': {
                'concept': 'Latitude',
                'uri': 'http://www.w3.org/2003/01/geo/wgs84_pos#lat',
                'data_type': 'float',
                'unit': 'degrees',
                'validation': ['range(-90, 90)']
            },
            'longitude': {
                'concept': 'Longitude',
                'uri': 'http://www.w3.org/2003/01/geo/wgs84_pos#long',
                'data_type': 'float', 
                'unit': 'degrees',
                'validation': ['range(-180, 180)']
            }
        }
        
        # Commodity mappings
        commodity_mappings = {
            'price': {
                'concept': 'CommodityPrice',
                'uri': str(COMMODITY.hasPrice),
                'data_type': 'float',
                'unit': 'usd_per_bushel',
                'validation': ['range(0, 1000)']
            },
            'commodity': {
                'concept': 'CommodityType',
                'uri': str(COMMODITY.commodityType),
                'data_type': 'string'
            }
        }
        
        # Search for field mapping
        all_mappings = {**hydro_mappings, **transport_mappings, **commodity_mappings}
        
        # Direct field name match
        if field_lower in all_mappings:
            return all_mappings[field_lower]
            
        # Partial matches
        for mapping_key, mapping_value in all_mappings.items():
            if mapping_key in field_lower or field_lower in mapping_key:
                return mapping_value
        
        return None
    
    def _extract_spatial_context(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract spatial context and coordinates"""
        
        spatial_info = {}
        
        # Look for coordinate fields
        lat_fields = [k for k in record.keys() if 'lat' in k.lower()]
        lon_fields = [k for k in record.keys() if 'lon' in k.lower() or 'lng' in k.lower()]
        
        if lat_fields and lon_fields:
            try:
                lat = float(record[lat_fields[0]])
                lon = float(record[lon_fields[0]])
                
                spatial_info = {
                    'latitude': lat,
                    'longitude': lon,
                    'coordinate_system': 'WGS84',
                    'spatial_precision': self._estimate_spatial_precision(lat, lon)
                }
                
                # Add river mile estimation for Mississippi River system
                if self._is_on_mississippi_system(lat, lon):
                    river_mile = self._estimate_river_mile(lat, lon)
                    spatial_info['river_mile'] = river_mile
                    spatial_info['waterway_system'] = 'Mississippi River'
                
            except (ValueError, TypeError):
                pass
        
        # Look for named location fields
        location_fields = [k for k in record.keys() if any(loc in k.lower() for loc in ['location', 'site', 'port', 'terminal'])]
        
        for field in location_fields:
            location_name = str(record[field])
            if location_name and len(location_name) > 2:
                geocoded = self._geocode_location(location_name)
                if geocoded:
                    spatial_info.update(geocoded)
                break
        
        return spatial_info if spatial_info else None
    
    def _extract_temporal_context(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract temporal context and timestamps"""
        
        temporal_info = {}
        
        # Look for timestamp fields
        time_fields = [k for k in record.keys() if any(time in k.lower() for time in ['time', 'date', 'timestamp'])]
        
        for field in time_fields:
            try:
                if pd.notna(record[field]):
                    # Convert pandas Timestamp to Python datetime for JSON serialization
                    timestamp = pd.to_datetime(record[field]).to_pydatetime()
                    temporal_info[f"{field}_parsed"] = timestamp
                    
                    # Add temporal classifications
                    if not temporal_info.get('primary_timestamp'):
                        temporal_info['primary_timestamp'] = timestamp
                        temporal_info['temporal_precision'] = self._classify_temporal_precision(str(record[field]))
                        temporal_info['data_freshness_hours'] = (datetime.now() - timestamp).total_seconds() / 3600
                        
            except (ValueError, TypeError):
                continue
        
        return temporal_info if temporal_info else None
    
    def _classify_domain_concepts(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Classify record into domain-specific concept categories"""
        
        classifications = []
        
        # Determine primary domain classification
        if self.semantic_context.domain == "hydrology":
            if any(field in record for field in ['water_level', 'stage', 'flow_rate', 'discharge']):
                classifications.append({
                    'category': 'hydrological_measurement',
                    'confidence': 0.9,
                    'evidence': 'presence_of_hydrological_parameters'
                })
                
        elif self.semantic_context.domain == "transportation":
            if any(field in record for field in ['vessel_name', 'mmsi', 'speed', 'heading']):
                classifications.append({
                    'category': 'vessel_tracking',
                    'confidence': 0.9,
                    'evidence': 'presence_of_vessel_parameters'
                })
                
        elif self.semantic_context.domain == "economics":
            if any(field in record for field in ['price', 'commodity', 'market']):
                classifications.append({
                    'category': 'market_data',
                    'confidence': 0.9,
                    'evidence': 'presence_of_market_parameters'
                })
        
        # Add cross-domain classifications
        if self._has_spatial_data(record):
            classifications.append({
                'category': 'geospatial_data',
                'confidence': 0.8,
                'evidence': 'presence_of_coordinates'
            })
            
        if self._has_temporal_data(record):
            classifications.append({
                'category': 'time_series_data',
                'confidence': 0.8,
                'evidence': 'presence_of_timestamps'
            })
        
        return classifications
    
    def _assess_data_quality(self, record: Dict[str, Any]) -> Dict[str, float]:
        """Assess data quality metrics"""
        
        total_fields = len(record)
        non_null_fields = sum(1 for v in record.values() if v is not None and v != '')
        
        quality_metrics = {
            'completeness': non_null_fields / total_fields if total_fields > 0 else 0,
            'consistency': self._check_consistency(record),
            'validity': self._check_validity(record),
            'accuracy': self._estimate_accuracy(record),
            'timeliness': self._assess_timeliness(record)
        }
        
        quality_metrics['overall_quality'] = np.mean(list(quality_metrics.values()))
        
        return quality_metrics
    
    def _create_interop_mappings(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Create interoperability mappings for cross-system integration"""
        
        return {
            'schema_version': '1.0',
            'source_system': self.source_name,
            'semantic_context': self.semantic_context.domain,
            'canonical_fields': self._map_to_canonical_schema(record),
            'linked_data_references': self._generate_linked_data_refs(record),
            'cross_reference_keys': self._identify_cross_reference_keys(record)
        }
    
    async def collect_semantically_enriched_data(self) -> List[StructuredRecord]:
        """Main ET(K)L collection method"""
        
        logger.info(f"Starting semantic data collection for {self.source_name}")
        
        # Extract raw data (E)
        raw_records = await self.extract_raw_data()
        
        enriched_records = []
        
        for i, raw_record in enumerate(raw_records):
            try:
                # Structure data (T) 
                structured_data = self.structure_data(raw_record)
                
                # Apply semantic enrichment (K)
                semantic_annotations = self.apply_semantic_enrichment(structured_data)
                
                # Convert any pandas Timestamps to Python datetime objects before creating record
                clean_structured_data = self._convert_timestamps_to_datetime(structured_data)
                clean_semantic_annotations = self._convert_timestamps_to_datetime(semantic_annotations)
                clean_spatial_info = self._convert_timestamps_to_datetime(semantic_annotations.get('spatial_context'))
                clean_temporal_info = self._convert_timestamps_to_datetime(semantic_annotations.get('temporal_context'))
                clean_quality_metrics = self._convert_timestamps_to_datetime(semantic_annotations.get('quality_assessment'))
                
                # Create enriched record
                record = StructuredRecord(
                    source_id=self.source_name,
                    record_id=f"{self.source_name}_{i}_{datetime.now().isoformat()}",
                    timestamp=datetime.now(),
                    raw_data=raw_record,
                    structured_data=clean_structured_data,
                    semantic_annotations=clean_semantic_annotations,
                    spatial_info=clean_spatial_info,
                    temporal_info=clean_temporal_info,
                    quality_metrics=clean_quality_metrics
                )
                
                enriched_records.append(record)
                
            except Exception as e:
                logger.error(f"Failed to process record {i} from {self.source_name}: {e}")
                continue
        
        logger.info(f"Collected {len(enriched_records)} semantically enriched records from {self.source_name}")
        
        return enriched_records
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for datetime and other non-serializable objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, 'to_pydatetime'):
            # Handle pandas Timestamp objects
            return obj.to_pydatetime().isoformat()
        elif hasattr(obj, '__dict__'):
            # Handle custom objects by returning their dict
            return obj.__dict__
        else:
            # Fallback to string representation
            return str(obj)
    
    def _convert_timestamps_to_datetime(self, data):
        """Recursively convert pandas Timestamps to Python datetime objects"""
        if isinstance(data, dict):
            return {k: self._convert_timestamps_to_datetime(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_timestamps_to_datetime(item) for item in data]
        elif hasattr(data, 'to_pydatetime'):
            # Convert pandas Timestamp to Python datetime
            return data.to_pydatetime()
        else:
            return data
    
    def _map_to_kuzu_schema(self, record: StructuredRecord, target_table: str) -> Dict[str, Any]:
        """Map enriched record to specific KuzuDB table schema"""
        
        if target_table == "HydroReading":
            # HydroReading schema: reading_id, segment_id, timestamp, water_level, flow_rate, temperature, ice_conditions, weather_impact, forecast_confidence
            return {
                'reading_id': record.record_id,
                'segment_id': record.structured_data.get('site_id', ''),
                'timestamp': record.timestamp.isoformat() if isinstance(record.timestamp, datetime) else str(record.timestamp),
                'water_level': float(record.structured_data.get('measured_value', 0)) if record.structured_data.get('measured_value') else 0.0,
                'flow_rate': float(record.structured_data.get('discharge', 0)) if record.structured_data.get('discharge') else 0.0,
                'temperature': float(record.structured_data.get('temperature', 0)) if record.structured_data.get('temperature') else 0.0,
                'ice_conditions': record.structured_data.get('ice_conditions', 'none'),
                'weather_impact': record.structured_data.get('weather_impact', 'normal'),
                'forecast_confidence': float(record.quality_metrics.get('overall_quality', 0.5)) if record.quality_metrics else 0.5
            }
            
        elif target_table == "VesselPosition":
            # VesselPosition schema: position_id, vessel_id, timestamp, latitude, longitude, speed, heading, status, destination, eta
            return {
                'position_id': record.record_id,
                'vessel_id': record.structured_data.get('mmsi', record.structured_data.get('vessel_id', '')),
                'timestamp': record.timestamp.isoformat() if isinstance(record.timestamp, datetime) else str(record.timestamp),
                'latitude': float(record.structured_data.get('latitude', 0)) if record.structured_data.get('latitude') else 0.0,
                'longitude': float(record.structured_data.get('longitude', 0)) if record.structured_data.get('longitude') else 0.0,
                'speed': float(record.structured_data.get('speed_over_ground', 0)) if record.structured_data.get('speed_over_ground') else 0.0,
                'heading': float(record.structured_data.get('heading', 0)) if record.structured_data.get('heading') else 0.0,
                'status': record.structured_data.get('navigation_status', 'unknown'),
                'destination': record.structured_data.get('destination', ''),
                'eta': record.structured_data.get('eta', record.timestamp.isoformat() if isinstance(record.timestamp, datetime) else str(record.timestamp))
            }
            
        else:
            # Generic mapping for unknown tables - just use basic fields
            return {
                'id': record.record_id,
                'source': record.source_id,
                'timestamp': record.timestamp.isoformat() if isinstance(record.timestamp, datetime) else str(record.timestamp),
                'data': json.dumps(record.structured_data, default=self._json_serializer)
            }

    def store_in_kuzu_tables(self, enriched_records: List[StructuredRecord], target_table: str):
        """Store semantically enriched data in KuzuDB tables (L in ET(K)L)"""
        
        if not enriched_records:
            return
            
        # Map enriched records to KuzuDB schema
        mapped_records = []
        
        for record in enriched_records:
            try:
                mapped_record = self._map_to_kuzu_schema(record, target_table)
                mapped_records.append(mapped_record)
            except Exception as e:
                logger.warning(f"Failed to map record {record.record_id} to {target_table} schema: {e}")
                continue
        
        if not mapped_records:
            logger.warning(f"No records successfully mapped to {target_table} schema")
            return
        
        # Create DataFrame and save as CSV for KuzuDB bulk loading
        df = pd.DataFrame(mapped_records)
        csv_path = f"/tmp/{target_table}_mapped.csv"
        df.to_csv(csv_path, index=False)
        
        try:
            # Load into KuzuDB
            self.temp_conn.execute(f"COPY {target_table} FROM '{csv_path}' (HEADER=true)")
            logger.info(f"Loaded {len(mapped_records)} records into {target_table}")
            
        except Exception as e:
            logger.error(f"Failed to load data into {target_table}: {e}")
            # Log the first few column names for debugging
            if len(df.columns) > 0:
                logger.error(f"DataFrame columns ({len(df.columns)}): {list(df.columns)[:10]}...")
            logger.error(f"Expected schema for {target_table} - check kuzu_navigation_schema.py")
    
    # Helper methods for semantic processing
    def _store_temp_entity(self, entity: Dict[str, Any]):
        """Store entity in temporary graph for cross-record resolution"""
        pass  # Implementation depends on specific entity types
    
    def _store_temp_concept(self, concept: Dict[str, Any]):
        """Store concept mapping in temporary graph"""
        pass  # Implementation for concept consistency
    
    def _resolve_spatial_coordinates(self, location_name: str) -> Optional[Dict[str, float]]:
        """Resolve location name to coordinates"""
        # Integration with geocoding services
        return None
    
    def _is_on_mississippi_system(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within Mississippi River system"""
        # Simplified bounding box check
        return 29.0 <= lat <= 47.9 and -95.2 <= lon <= -89.0
    
    def _estimate_river_mile(self, lat: float, lon: float) -> float:
        """Estimate river mile from coordinates"""
        # Simplified linear estimation
        head_lat = 44.98  # Minneapolis
        mouth_lat = 29.95  # New Orleans
        
        lat_progress = (head_lat - lat) / (head_lat - mouth_lat)
        return max(0, min(2320, lat_progress * 2320))
    
    def _geocode_location(self, location_name: str) -> Optional[Dict[str, Any]]:
        """Geocode location name to coordinates"""
        # Integration with geocoding APIs
        return None
    
    def _classify_temporal_precision(self, timestamp_str: str) -> str:
        """Classify temporal precision of timestamp"""
        if 'T' in timestamp_str and ':' in timestamp_str:
            return 'datetime'
        elif '-' in timestamp_str:
            return 'date'
        else:
            return 'unknown'
    
    def _has_spatial_data(self, record: Dict[str, Any]) -> bool:
        """Check if record contains spatial data"""
        spatial_indicators = ['lat', 'lon', 'location', 'coordinate']
        return any(indicator in str(key).lower() for key in record.keys() for indicator in spatial_indicators)
    
    def _has_temporal_data(self, record: Dict[str, Any]) -> bool:
        """Check if record contains temporal data"""
        temporal_indicators = ['time', 'date', 'timestamp']
        return any(indicator in str(key).lower() for key in record.keys() for indicator in temporal_indicators)
    
    def _check_consistency(self, record: Dict[str, Any]) -> float:
        """Check data consistency"""
        # Implementation for consistency validation
        return 0.8
    
    def _check_validity(self, record: Dict[str, Any]) -> float:
        """Check data validity against domain rules"""
        # Implementation for validity checks
        return 0.9
    
    def _estimate_accuracy(self, record: Dict[str, Any]) -> float:
        """Estimate data accuracy"""
        # Implementation for accuracy estimation
        return 0.8
    
    def _assess_timeliness(self, record: Dict[str, Any]) -> float:
        """Assess data timeliness"""
        # Implementation for timeliness assessment
        return 0.9
    
    def _map_to_canonical_schema(self, record: Dict[str, Any]) -> Dict[str, str]:
        """Map fields to canonical schema"""
        # Implementation for canonical mapping
        return {}
    
    def _generate_linked_data_refs(self, record: Dict[str, Any]) -> List[str]:
        """Generate linked data references"""
        # Implementation for linked data generation
        return []
    
    def _identify_cross_reference_keys(self, record: Dict[str, Any]) -> List[str]:
        """Identify keys for cross-referencing with other systems"""
        # Implementation for cross-reference identification
        return []
    
    def _estimate_spatial_precision(self, lat: float, lon: float) -> str:
        """Estimate spatial precision of coordinates"""
        # Check decimal places to estimate precision
        lat_decimals = len(str(lat).split('.')[-1]) if '.' in str(lat) else 0
        lon_decimals = len(str(lon).split('.')[-1]) if '.' in str(lon) else 0
        
        avg_decimals = (lat_decimals + lon_decimals) / 2
        
        if avg_decimals >= 6:
            return 'high'  # ~1 meter
        elif avg_decimals >= 4:
            return 'medium'  # ~10 meters  
        else:
            return 'low'  # >100 meters