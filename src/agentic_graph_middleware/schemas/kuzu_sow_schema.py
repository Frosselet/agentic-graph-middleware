"""
Revolutionary Semantic SOW Schema with KuzuDB as Primary Graph Engine

This module implements a KuzuDB-first architecture for Statement of Work (SOW) inference
with engaging graph visualization capabilities. KuzuDB handles all graph storage, primary
analytics, and scales to enterprise needs. Modern web visualization creates engaging 
experiences. NetworkX only fills specific algorithm gaps until KuzuDB adds those capabilities.

Core Design Principles:
1. KuzuDB-First: All graph storage, querying, and primary analytics in KuzuDB
2. Engaging Visualization: Interactive graph exploration for business users
3. Intelligent Inference: Graph traversal to discover implicit requirements
4. 4D Navigation: Time-space-domain-knowledge layers
"""

import kuzu
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, asdict
from pathlib import Path
import asyncio
from contextlib import asynccontextmanager

# Import minimal NetworkX for specific algorithms not yet in KuzuDB
import networkx as nx


logger = logging.getLogger(__name__)


@dataclass
class BusinessRequirement:
    """Business requirement node in the SOW graph"""
    id: str
    description: str
    priority: int  # 1-5 scale
    domain: str
    complexity: str  # 'low', 'medium', 'high', 'very_high'
    estimated_hours: Optional[int] = None
    business_value: Optional[float] = None
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass 
class AnalyticalOpportunity:
    """Analytical opportunity discovered through graph inference"""
    id: str
    description: str
    complexity: str
    business_value: float
    confidence_score: float  # 0.0-1.0 inference confidence
    discovery_method: str  # 'graph_traversal', 'pattern_matching', 'cross_domain'
    related_requirements: List[str]  # IDs of related requirements
    implementation_approach: str
    estimated_hours: int
    roi_projection: Optional[float] = None
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class DomainEntity:
    """Business domain entity (company, department, system, etc.)"""
    id: str
    name: str
    entity_type: str  # 'company', 'department', 'system', 'process'
    industry: str
    maturity_level: str  # 'startup', 'growth', 'mature', 'enterprise'
    technology_stack: List[str]
    data_maturity: str  # 'ad_hoc', 'defined', 'managed', 'optimized'
    
    
@dataclass
class InferenceRule:
    """Rules for discovering implicit opportunities from explicit requirements"""
    id: str
    rule_type: str  # 'implication', 'correlation', 'sequence', 'prerequisite'
    condition_pattern: str  # Graph pattern to match
    conclusion_template: str  # Template for generating opportunities
    confidence_weight: float  # 0.0-1.0 confidence modifier
    domain_applicability: List[str]  # Domains where rule applies
    success_rate: float  # Historical success rate of this rule


class KuzuSOWGraphEngine:
    """
    KuzuDB-powered SOW inference engine with engaging graph analytics.
    
    This class implements the core graph database operations for SOW analysis,
    using KuzuDB as the primary graph engine for high-performance analytics.
    """
    
    def __init__(self, database_path: str = "sow_graph.db"):
        """Initialize KuzuDB connection and set up graph schema"""
        self.db_path = database_path
        self.conn = None
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize KuzuDB database and create graph schema"""
        try:
            # Create KuzuDB database
            self.db = kuzu.Database(self.db_path)
            self.conn = kuzu.Connection(self.db)
            
            logger.info(f"Initialized KuzuDB at {self.db_path}")
            self._create_graph_schema()
            self._create_inference_rules()
            
        except Exception as e:
            logger.error(f"Failed to initialize KuzuDB: {e}")
            raise
    
    def _create_graph_schema(self):
        """Create comprehensive graph schema for SOW inference"""
        
        # Node types
        schema_queries = [
            # Business Requirements
            """
            CREATE NODE TABLE IF NOT EXISTS BusinessRequirement(
                id STRING,
                description STRING,
                priority INT16,
                domain STRING,
                complexity STRING,
                estimated_hours INT32,
                business_value DOUBLE,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY(id)
            )
            """,
            
            # Analytical Opportunities
            """
            CREATE NODE TABLE IF NOT EXISTS AnalyticalOpportunity(
                id STRING,
                description STRING,
                complexity STRING,
                business_value DOUBLE,
                confidence_score DOUBLE,
                discovery_method STRING,
                implementation_approach STRING,
                estimated_hours INT32,
                roi_projection DOUBLE,
                created_at TIMESTAMP,
                status STRING,
                PRIMARY KEY(id)
            )
            """,
            
            # Domain Entities
            """
            CREATE NODE TABLE IF NOT EXISTS DomainEntity(
                id STRING,
                name STRING,
                entity_type STRING,
                industry STRING,
                maturity_level STRING,
                technology_stack STRING[],
                data_maturity STRING,
                geographic_region STRING,
                employee_count INT32,
                revenue_range STRING,
                PRIMARY KEY(id)
            )
            """,
            
            # Inference Rules
            """
            CREATE NODE TABLE IF NOT EXISTS InferenceRule(
                id STRING,
                rule_type STRING,
                condition_pattern STRING,
                conclusion_template STRING,
                confidence_weight DOUBLE,
                domain_applicability STRING[],
                success_rate DOUBLE,
                usage_count INT32,
                last_applied TIMESTAMP,
                PRIMARY KEY(id)
            )
            """,
            
            # Knowledge Patterns (for pattern matching)
            """
            CREATE NODE TABLE IF NOT EXISTS KnowledgePattern(
                id STRING,
                pattern_name STRING,
                pattern_type STRING,
                graph_structure STRING,
                business_context STRING,
                success_indicators STRING[],
                complexity_score DOUBLE,
                applicability_domains STRING[],
                PRIMARY KEY(id)
            )
            """,
            
            # Relationship tables
            """
            CREATE REL TABLE IF NOT EXISTS IMPLIES(
                FROM BusinessRequirement TO AnalyticalOpportunity,
                confidence DOUBLE,
                reasoning STRING,
                inference_path STRING,
                created_at TIMESTAMP
            )
            """,
            
            """
            CREATE REL TABLE IF NOT EXISTS ENABLES(
                FROM DomainEntity TO AnalyticalOpportunity,
                enablement_type STRING,
                readiness_score DOUBLE,
                prerequisites STRING[],
                estimated_timeline_days INT32
            )
            """,
            
            """
            CREATE REL TABLE IF NOT EXISTS DEPENDS_ON(
                FROM AnalyticalOpportunity TO BusinessRequirement,
                dependency_type STRING,
                criticality STRING,
                implementation_order INT16
            )
            """,
            
            """
            CREATE REL TABLE IF NOT EXISTS BELONGS_TO(
                FROM BusinessRequirement TO DomainEntity,
                ownership_level STRING,
                stakeholder_priority DOUBLE
            )
            """,
            
            """
            CREATE REL TABLE IF NOT EXISTS CORRELATES_WITH(
                FROM AnalyticalOpportunity TO AnalyticalOpportunity,
                correlation_strength DOUBLE,
                correlation_type STRING,
                synergy_potential DOUBLE
            )
            """,
            
            """
            CREATE REL TABLE IF NOT EXISTS MATCHES_PATTERN(
                FROM BusinessRequirement TO KnowledgePattern,
                match_confidence DOUBLE,
                pattern_completeness DOUBLE,
                contextual_fit STRING
            )
            """,
            
            """
            CREATE REL TABLE IF NOT EXISTS GENERATES(
                FROM InferenceRule TO AnalyticalOpportunity,
                application_context STRING,
                confidence_used DOUBLE,
                validation_status STRING
            )
            """
        ]
        
        for query in schema_queries:
            try:
                self.conn.execute(query)
                logger.debug(f"Executed schema query: {query[:50]}...")
            except Exception as e:
                logger.error(f"Failed to execute schema query: {e}")
                logger.debug(f"Query: {query}")
    
    def _create_inference_rules(self):
        """Create comprehensive inference rules for SOW opportunity discovery"""
        
        inference_rules = [
            InferenceRule(
                id="RULE_001",
                rule_type="implication", 
                condition_pattern="data_collection",
                conclusion_template="Data quality assessment and profiling capabilities",
                confidence_weight=0.9,
                domain_applicability=["finance", "healthcare", "manufacturing", "retail"],
                success_rate=0.85
            ),
            
            InferenceRule(
                id="RULE_002",
                rule_type="sequence",
                condition_pattern="supplier_tracking",
                conclusion_template="Supply chain risk analytics and predictive monitoring",
                confidence_weight=0.8,
                domain_applicability=["manufacturing", "retail", "logistics"],
                success_rate=0.78
            ),
            
            InferenceRule(
                id="RULE_003", 
                rule_type="correlation",
                condition_pattern="customer_data",
                conclusion_template="Customer segmentation and behavioral analytics",
                confidence_weight=0.85,
                domain_applicability=["retail", "e-commerce", "financial_services"],
                success_rate=0.82
            ),
            
            InferenceRule(
                id="RULE_004",
                rule_type="prerequisite",
                condition_pattern="reporting_automation",
                conclusion_template="Data governance framework and metadata management",
                confidence_weight=0.75,
                domain_applicability=["all"],
                success_rate=0.70
            ),
            
            InferenceRule(
                id="RULE_005",
                rule_type="implication",
                condition_pattern="regulatory_compliance",
                conclusion_template="Automated compliance monitoring and audit trails",
                confidence_weight=0.9,
                domain_applicability=["finance", "healthcare", "insurance"],
                success_rate=0.88
            )
        ]
        
        for rule in inference_rules:
            self.add_inference_rule(rule)
    
    def add_business_requirement(self, requirement: BusinessRequirement) -> bool:
        """Add a business requirement to the graph"""
        try:
            query = """
            CREATE (:BusinessRequirement {
                id: $id,
                description: $description,
                priority: $priority,
                domain: $domain,
                complexity: $complexity,
                estimated_hours: $estimated_hours,
                business_value: $business_value,
                created_at: $created_at,
                updated_at: $created_at
            })
            """
            
            params = asdict(requirement)
            params['created_at'] = requirement.created_at.isoformat()
            
            self.conn.execute(query, params)
            logger.info(f"Added business requirement: {requirement.id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add business requirement {requirement.id}: {e}")
            return False
    
    def add_domain_entity(self, entity: DomainEntity) -> bool:
        """Add a domain entity to the graph"""
        try:
            query = """
            CREATE (:DomainEntity {
                id: $id,
                name: $name,
                entity_type: $entity_type,
                industry: $industry,
                maturity_level: $maturity_level,
                technology_stack: $technology_stack,
                data_maturity: $data_maturity
            })
            """
            
            params = asdict(entity)
            self.conn.execute(query, params)
            logger.info(f"Added domain entity: {entity.id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add domain entity {entity.id}: {e}")
            return False
    
    def add_inference_rule(self, rule: InferenceRule) -> bool:
        """Add an inference rule to the graph"""
        try:
            query = """
            CREATE (:InferenceRule {
                id: $id,
                rule_type: $rule_type,
                condition_pattern: $condition_pattern,
                conclusion_template: $conclusion_template,
                confidence_weight: $confidence_weight,
                domain_applicability: $domain_applicability,
                success_rate: $success_rate,
                usage_count: 0,
                last_applied: $now
            })
            """
            
            params = asdict(rule)
            params['now'] = datetime.now().isoformat()
            
            self.conn.execute(query, params)
            logger.info(f"Added inference rule: {rule.id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add inference rule {rule.id}: {e}")
            return False
    
    def discover_implicit_opportunities(self, requirement_id: str) -> List[AnalyticalOpportunity]:
        """
        Discover implicit analytical opportunities through KuzuDB graph traversal.
        
        This is the core intelligence of the SOW system - using graph patterns
        and inference rules to discover opportunities that weren't explicitly requested.
        """
        opportunities = []
        
        try:
            # Get the requirement details
            req_query = """
            MATCH (req:BusinessRequirement {id: $req_id})
            RETURN req.description, req.domain, req.complexity
            """
            
            req_result = self.conn.execute(req_query, {"req_id": requirement_id})
            
            if not req_result.get_next():
                logger.warning(f"Requirement {requirement_id} not found")
                return opportunities
            
            req_data = req_result.get_next()
            req_description = req_data[0].lower()
            req_domain = req_data[1]
            req_complexity = req_data[2]
            
            # Apply inference rules through graph traversal
            inference_query = """
            MATCH (rule:InferenceRule)
            WHERE $req_domain IN rule.domain_applicability OR 'all' IN rule.domain_applicability
            AND rule.success_rate > 0.6
            RETURN rule.id, rule.condition_pattern, rule.conclusion_template, 
                   rule.confidence_weight, rule.success_rate
            ORDER BY rule.confidence_weight DESC, rule.success_rate DESC
            """
            
            inference_result = self.conn.execute(inference_query, {"req_domain": req_domain})
            
            opportunity_counter = 1
            for rule_record in inference_result:
                rule_id = rule_record[0]
                condition_pattern = rule_record[1].lower()
                conclusion_template = rule_record[2]
                confidence_weight = rule_record[3]
                success_rate = rule_record[4]
                
                # Check if requirement description matches the condition pattern
                if condition_pattern in req_description or any(
                    word in req_description for word in condition_pattern.split()
                ):
                    # Generate analytical opportunity
                    opportunity = AnalyticalOpportunity(
                        id=f"{requirement_id}_OPP_{opportunity_counter:03d}",
                        description=conclusion_template,
                        complexity=self._infer_complexity(req_complexity, confidence_weight),
                        business_value=self._calculate_business_value(confidence_weight, success_rate),
                        confidence_score=confidence_weight * success_rate,
                        discovery_method="graph_traversal",
                        related_requirements=[requirement_id],
                        implementation_approach=self._suggest_implementation(conclusion_template),
                        estimated_hours=self._estimate_hours(conclusion_template, req_complexity)
                    )
                    
                    opportunities.append(opportunity)
                    
                    # Add opportunity to graph and create relationship
                    self._add_discovered_opportunity(opportunity, requirement_id, rule_id)
                    
                    opportunity_counter += 1
            
            # Cross-domain opportunity discovery
            cross_domain_opportunities = self._discover_cross_domain_opportunities(
                requirement_id, req_domain, req_description
            )
            opportunities.extend(cross_domain_opportunities)
            
            logger.info(f"Discovered {len(opportunities)} opportunities for requirement {requirement_id}")
            return opportunities
            
        except Exception as e:
            logger.error(f"Failed to discover opportunities for {requirement_id}: {e}")
            return opportunities
    
    def _discover_cross_domain_opportunities(self, req_id: str, req_domain: str, req_description: str) -> List[AnalyticalOpportunity]:
        """Discover opportunities by analyzing related domains and entities"""
        opportunities = []
        
        try:
            # Find related domain entities that could enable new opportunities
            cross_domain_query = """
            MATCH (entity:DomainEntity)
            WHERE entity.industry <> $req_domain
            AND (entity.maturity_level IN ['mature', 'enterprise'])
            AND entity.data_maturity IN ['managed', 'optimized']
            RETURN entity.id, entity.name, entity.industry, entity.technology_stack
            LIMIT 5
            """
            
            result = self.conn.execute(cross_domain_query, {"req_domain": req_domain})
            
            opportunity_counter = 1
            for record in result:
                entity_id = record[0]
                entity_name = record[1] 
                entity_industry = record[2]
                tech_stack = record[3] if record[3] else []
                
                # Generate cross-domain opportunity based on entity capabilities
                opportunity = AnalyticalOpportunity(
                    id=f"{req_id}_CROSS_{opportunity_counter:03d}",
                    description=f"Cross-domain analytics leveraging {entity_industry} domain patterns for enhanced {req_domain} insights",
                    complexity="medium",
                    business_value=self._calculate_cross_domain_value(req_domain, entity_industry),
                    confidence_score=0.7,  # Lower confidence for cross-domain
                    discovery_method="cross_domain",
                    related_requirements=[req_id],
                    implementation_approach=f"Leverage {entity_industry} domain expertise and {', '.join(tech_stack[:3])} technologies",
                    estimated_hours=self._estimate_cross_domain_hours(req_description)
                )
                
                opportunities.append(opportunity)
                self._add_discovered_opportunity(opportunity, req_id, f"CROSS_DOMAIN_{entity_id}")
                
                opportunity_counter += 1
                
        except Exception as e:
            logger.error(f"Failed cross-domain discovery: {e}")
            
        return opportunities
    
    def _add_discovered_opportunity(self, opportunity: AnalyticalOpportunity, requirement_id: str, source_id: str):
        """Add discovered opportunity to graph with relationships"""
        try:
            # Add opportunity node
            opp_query = """
            CREATE (:AnalyticalOpportunity {
                id: $id,
                description: $description,
                complexity: $complexity,
                business_value: $business_value,
                confidence_score: $confidence_score,
                discovery_method: $discovery_method,
                implementation_approach: $implementation_approach,
                estimated_hours: $estimated_hours,
                roi_projection: $roi_projection,
                created_at: $created_at,
                status: 'discovered'
            })
            """
            
            opp_params = asdict(opportunity)
            opp_params['created_at'] = opportunity.created_at.isoformat()
            
            self.conn.execute(opp_query, opp_params)
            
            # Create IMPLIES relationship
            implies_query = """
            MATCH (req:BusinessRequirement {id: $req_id})
            MATCH (opp:AnalyticalOpportunity {id: $opp_id})
            CREATE (req)-[:IMPLIES {
                confidence: $confidence,
                reasoning: $reasoning,
                inference_path: $inference_path,
                created_at: $created_at
            }]->(opp)
            """
            
            self.conn.execute(implies_query, {
                "req_id": requirement_id,
                "opp_id": opportunity.id,
                "confidence": opportunity.confidence_score,
                "reasoning": f"Discovered via {opportunity.discovery_method}",
                "inference_path": f"REQ:{requirement_id} -> SOURCE:{source_id} -> OPP:{opportunity.id}",
                "created_at": datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Failed to add discovered opportunity {opportunity.id}: {e}")
    
    def _infer_complexity(self, req_complexity: str, confidence: float) -> str:
        """Infer opportunity complexity based on requirement and confidence"""
        complexity_map = {
            "low": ["low", "medium"] if confidence > 0.8 else ["low"],
            "medium": ["low", "medium", "high"] if confidence > 0.7 else ["medium"],
            "high": ["medium", "high", "very_high"] if confidence > 0.8 else ["high"],
            "very_high": ["high", "very_high"]
        }
        
        options = complexity_map.get(req_complexity, ["medium"])
        # For inference, typically add some complexity
        return options[-1] if len(options) > 1 else options[0]
    
    def _calculate_business_value(self, confidence: float, success_rate: float) -> float:
        """Calculate business value score based on confidence and historical success"""
        base_value = 1000  # Base value in arbitrary business units
        multiplier = confidence * success_rate * 1.5  # Boost for high confidence + success
        return round(base_value * multiplier, 2)
    
    def _calculate_cross_domain_value(self, source_domain: str, target_domain: str) -> float:
        """Calculate business value for cross-domain opportunities"""
        # Cross-domain opportunities often have higher value due to novel insights
        domain_synergies = {
            ("finance", "healthcare"): 1.4,
            ("manufacturing", "retail"): 1.3,
            ("healthcare", "insurance"): 1.5,
            ("retail", "logistics"): 1.2
        }
        
        base_value = 1200  # Higher base for cross-domain
        synergy = domain_synergies.get((source_domain, target_domain), 1.1)
        return round(base_value * synergy, 2)
    
    def _suggest_implementation(self, description: str) -> str:
        """Suggest implementation approach based on opportunity description"""
        if "quality" in description.lower():
            return "Implement data profiling tools with automated quality checks and remediation workflows"
        elif "risk" in description.lower():
            return "Deploy predictive analytics models with real-time monitoring dashboards"
        elif "segmentation" in description.lower():
            return "Develop machine learning clustering models with behavioral analysis"
        elif "compliance" in description.lower():
            return "Build automated compliance monitoring with audit trail management"
        else:
            return "Implement analytics platform with custom dashboards and reporting capabilities"
    
    def _estimate_hours(self, description: str, complexity: str) -> int:
        """Estimate implementation hours based on description and complexity"""
        base_hours = {
            "low": 80,
            "medium": 160, 
            "high": 320,
            "very_high": 480
        }
        
        complexity_hours = base_hours.get(complexity, 160)
        
        # Adjust based on description keywords
        if "automation" in description.lower():
            complexity_hours *= 1.3
        if "machine learning" in description.lower():
            complexity_hours *= 1.4
        if "real-time" in description.lower():
            complexity_hours *= 1.2
            
        return int(complexity_hours)
    
    def _estimate_cross_domain_hours(self, description: str) -> int:
        """Estimate hours for cross-domain opportunities (typically more complex)"""
        base_hours = 240  # Higher base for cross-domain complexity
        
        if "integration" in description.lower():
            base_hours *= 1.3
        if "correlation" in description.lower():
            base_hours *= 1.2
            
        return int(base_hours)
    
    def get_opportunity_graph_data(self, requirement_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get graph data optimized for visualization.
        
        Returns nodes and edges in format suitable for Cytoscape.js/D3.js visualization.
        """
        try:
            if requirement_id:
                # Get specific requirement and its opportunities
                query = """
                MATCH (req:BusinessRequirement {id: $req_id})
                OPTIONAL MATCH (req)-[impl:IMPLIES]->(opp:AnalyticalOpportunity)
                OPTIONAL MATCH (req)-[belongs:BELONGS_TO]->(entity:DomainEntity)
                RETURN req, impl, opp, belongs, entity
                """
                params = {"req_id": requirement_id}
            else:
                # Get overview of all requirements and opportunities
                query = """
                MATCH (req:BusinessRequirement)
                OPTIONAL MATCH (req)-[impl:IMPLIES]->(opp:AnalyticalOpportunity)
                OPTIONAL MATCH (req)-[belongs:BELONGS_TO]->(entity:DomainEntity)
                RETURN req, impl, opp, belongs, entity
                LIMIT 50
                """
                params = {}
            
            result = self.conn.execute(query, params)
            
            nodes = []
            edges = []
            node_ids = set()
            
            for record in result:
                req = record[0]
                impl_rel = record[1]  
                opp = record[2]
                belongs_rel = record[3]
                entity = record[4]
                
                # Add requirement node
                if req and req.get('id') not in node_ids:
                    nodes.append({
                        'data': {
                            'id': req.get('id'),
                            'label': req.get('description', '')[:50] + '...',
                            'type': 'requirement',
                            'priority': req.get('priority', 3),
                            'domain': req.get('domain', ''),
                            'complexity': req.get('complexity', 'medium')
                        }
                    })
                    node_ids.add(req.get('id'))
                
                # Add opportunity node and relationship
                if opp and opp.get('id') not in node_ids:
                    nodes.append({
                        'data': {
                            'id': opp.get('id'),
                            'label': opp.get('description', '')[:50] + '...',
                            'type': 'opportunity',
                            'business_value': opp.get('business_value', 0),
                            'confidence_score': opp.get('confidence_score', 0),
                            'discovery_method': opp.get('discovery_method', ''),
                            'status': opp.get('status', 'discovered')
                        }
                    })
                    node_ids.add(opp.get('id'))
                
                # Add IMPLIES edge
                if impl_rel and req and opp:
                    edges.append({
                        'data': {
                            'id': f"{req.get('id')}_implies_{opp.get('id')}",
                            'source': req.get('id'),
                            'target': opp.get('id'),
                            'type': 'implies',
                            'confidence': impl_rel.get('confidence', 0),
                            'reasoning': impl_rel.get('reasoning', '')
                        }
                    })
                
                # Add entity node and relationship
                if entity and entity.get('id') not in node_ids:
                    nodes.append({
                        'data': {
                            'id': entity.get('id'),
                            'label': entity.get('name', ''),
                            'type': 'entity',
                            'entity_type': entity.get('entity_type', ''),
                            'industry': entity.get('industry', ''),
                            'maturity_level': entity.get('maturity_level', '')
                        }
                    })
                    node_ids.add(entity.get('id'))
                
                # Add BELONGS_TO edge
                if belongs_rel and req and entity:
                    edges.append({
                        'data': {
                            'id': f"{req.get('id')}_belongs_{entity.get('id')}",
                            'source': req.get('id'),
                            'target': entity.get('id'),
                            'type': 'belongs_to',
                            'ownership_level': belongs_rel.get('ownership_level', ''),
                            'stakeholder_priority': belongs_rel.get('stakeholder_priority', 0)
                        }
                    })
            
            return {
                'nodes': nodes,
                'edges': edges,
                'stats': {
                    'total_nodes': len(nodes),
                    'total_edges': len(edges),
                    'requirements_count': len([n for n in nodes if n['data']['type'] == 'requirement']),
                    'opportunities_count': len([n for n in nodes if n['data']['type'] == 'opportunity']),
                    'entities_count': len([n for n in nodes if n['data']['type'] == 'entity'])
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get graph data: {e}")
            return {'nodes': [], 'edges': [], 'stats': {}}
    
    def get_analytics_dashboard_data(self) -> Dict[str, Any]:
        """Get real-time analytics data for the SOW dashboard"""
        try:
            analytics_queries = {
                'discovery_metrics': """
                MATCH (opp:AnalyticalOpportunity)
                RETURN opp.discovery_method as method, 
                       count(*) as count,
                       avg(opp.confidence_score) as avg_confidence,
                       sum(opp.business_value) as total_value
                """,
                
                'complexity_distribution': """
                MATCH (req:BusinessRequirement)
                RETURN req.complexity as complexity, count(*) as count
                """,
                
                'domain_coverage': """
                MATCH (req:BusinessRequirement)
                RETURN req.domain as domain, count(*) as req_count
                """,
                
                'high_value_opportunities': """
                MATCH (opp:AnalyticalOpportunity)
                WHERE opp.business_value > 1000
                RETURN opp.id, opp.description, opp.business_value, opp.confidence_score
                ORDER BY opp.business_value DESC
                LIMIT 5
                """,
                
                'inference_performance': """
                MATCH (rule:InferenceRule)
                RETURN rule.rule_type as type,
                       avg(rule.success_rate) as avg_success_rate,
                       avg(rule.confidence_weight) as avg_confidence,
                       sum(rule.usage_count) as total_usage
                """
            }
            
            dashboard_data = {}
            
            for metric_name, query in analytics_queries.items():
                try:
                    result = self.conn.execute(query)
                    records = []
                    for record in result:
                        records.append(list(record))
                    dashboard_data[metric_name] = records
                except Exception as e:
                    logger.error(f"Failed to execute {metric_name} query: {e}")
                    dashboard_data[metric_name] = []
            
            return dashboard_data
            
        except Exception as e:
            logger.error(f"Failed to get analytics dashboard data: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
        if hasattr(self, 'db'):
            del self.db


class NetworkXBridge:
    """
    Minimal NetworkX integration for specific algorithms not yet available in KuzuDB.
    
    This bridge exports small subgraphs from KuzuDB to NetworkX, runs specific
    algorithms, and imports results back to KuzuDB. It's designed to be a temporary
    solution until KuzuDB adds more native graph algorithms.
    """
    
    def __init__(self, kuzu_engine: KuzuSOWGraphEngine):
        self.kuzu_engine = kuzu_engine
        self.logger = logging.getLogger(__name__)
    
    def run_centrality_analysis(self, requirement_id: Optional[str] = None) -> Dict[str, float]:
        """
        Run centrality analysis using NetworkX (until KuzuDB supports it natively).
        
        Returns centrality scores for nodes in the graph.
        """
        try:
            # Export subgraph from KuzuDB
            if requirement_id:
                query = """
                MATCH (req:BusinessRequirement {id: $req_id})-[r]-(connected)
                RETURN req.id as source, type(r) as rel_type, connected.id as target
                """
                params = {"req_id": requirement_id}
            else:
                query = """
                MATCH (source)-[r]-(target)
                RETURN source.id as source, type(r) as rel_type, target.id as target
                LIMIT 1000
                """
                params = {}
            
            result = self.kuzu_engine.conn.execute(query, params)
            
            # Build NetworkX graph
            G = nx.Graph()
            for record in result:
                source = record[0]
                target = record[2]
                rel_type = record[1]
                
                if source and target:
                    G.add_edge(source, target, relationship=rel_type)
            
            if len(G.nodes()) == 0:
                self.logger.warning("No nodes found for centrality analysis")
                return {}
            
            # Run centrality algorithms
            centrality_scores = {}
            
            if len(G.nodes()) > 1:
                # Betweenness centrality
                betweenness = nx.betweenness_centrality(G)
                # Degree centrality
                degree = nx.degree_centrality(G)
                # Closeness centrality (if graph is connected)
                if nx.is_connected(G):
                    closeness = nx.closeness_centrality(G)
                else:
                    closeness = {}
                
                # Combine centrality measures
                for node in G.nodes():
                    centrality_scores[node] = {
                        'betweenness': betweenness.get(node, 0),
                        'degree': degree.get(node, 0),
                        'closeness': closeness.get(node, 0),
                        'combined': (
                            betweenness.get(node, 0) * 0.4 +
                            degree.get(node, 0) * 0.3 +
                            closeness.get(node, 0) * 0.3
                        )
                    }
            
            # Import results back to KuzuDB
            self._import_centrality_scores(centrality_scores)
            
            self.logger.info(f"Analyzed centrality for {len(centrality_scores)} nodes")
            return centrality_scores
            
        except Exception as e:
            self.logger.error(f"Failed centrality analysis: {e}")
            return {}
    
    def _import_centrality_scores(self, centrality_scores: Dict[str, Dict[str, float]]):
        """Import centrality scores back to KuzuDB"""
        try:
            for node_id, scores in centrality_scores.items():
                # Update requirement nodes
                update_query = """
                MATCH (req:BusinessRequirement {id: $node_id})
                SET req.centrality_betweenness = $betweenness,
                    req.centrality_degree = $degree,
                    req.centrality_closeness = $closeness,
                    req.centrality_combined = $combined,
                    req.centrality_updated = $timestamp
                """
                
                self.kuzu_engine.conn.execute(update_query, {
                    "node_id": node_id,
                    "betweenness": scores['betweenness'],
                    "degree": scores['degree'],
                    "closeness": scores['closeness'],
                    "combined": scores['combined'],
                    "timestamp": datetime.now().isoformat()
                })
                
                # Also try updating opportunity nodes
                update_opp_query = """
                MATCH (opp:AnalyticalOpportunity {id: $node_id})
                SET opp.centrality_betweenness = $betweenness,
                    opp.centrality_degree = $degree,
                    opp.centrality_closeness = $closeness,
                    opp.centrality_combined = $combined,
                    opp.centrality_updated = $timestamp
                """
                
                try:
                    self.kuzu_engine.conn.execute(update_opp_query, {
                        "node_id": node_id,
                        "betweenness": scores['betweenness'],
                        "degree": scores['degree'],
                        "closeness": scores['closeness'],
                        "combined": scores['combined'],
                        "timestamp": datetime.now().isoformat()
                    })
                except:
                    pass  # Node might not be an opportunity
                    
        except Exception as e:
            self.logger.error(f"Failed to import centrality scores: {e}")
    
    def find_critical_paths(self, start_req_id: str, end_opp_id: str) -> List[List[str]]:
        """
        Find critical paths between requirement and opportunity using NetworkX.
        
        This is useful until KuzuDB adds native path-finding algorithms.
        """
        try:
            # Export subgraph focused on paths between start and end
            query = """
            MATCH path = (start:BusinessRequirement {id: $start_id})
                        -[*1..4]-
                        (end:AnalyticalOpportunity {id: $end_id})
            UNWIND relationships(path) as rel
            UNWIND nodes(path) as node
            RETURN startNode(rel).id as source, endNode(rel).id as target, type(rel) as rel_type
            """
            
            result = self.kuzu_engine.conn.execute(query, {
                "start_id": start_req_id,
                "end_id": end_opp_id
            })
            
            # Build directed NetworkX graph
            G = nx.DiGraph()
            for record in result:
                source = record[0]
                target = record[1]
                rel_type = record[2]
                
                if source and target:
                    G.add_edge(source, target, relationship=rel_type)
            
            # Find all simple paths
            try:
                paths = list(nx.all_simple_paths(G, start_req_id, end_opp_id, cutoff=4))
                self.logger.info(f"Found {len(paths)} paths from {start_req_id} to {end_opp_id}")
                return paths
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                self.logger.warning(f"No paths found from {start_req_id} to {end_opp_id}")
                return []
            
        except Exception as e:
            self.logger.error(f"Failed to find critical paths: {e}")
            return []


# Example usage and testing
if __name__ == "__main__":
    # Initialize the SOW graph engine
    engine = KuzuSOWGraphEngine("example_sow.db")
    
    # Add some example data
    req = BusinessRequirement(
        id="REQ_001",
        description="Implement supplier tracking system for automotive parts",
        priority=1,
        domain="manufacturing",
        complexity="high",
        estimated_hours=200,
        business_value=5000.0
    )
    
    entity = DomainEntity(
        id="ENT_001",
        name="AutoParts Manufacturing Corp",
        entity_type="company",
        industry="manufacturing",
        maturity_level="enterprise",
        technology_stack=["SAP", "Oracle", "AWS"],
        data_maturity="managed"
    )
    
    engine.add_business_requirement(req)
    engine.add_domain_entity(entity)
    
    # Discover opportunities
    opportunities = engine.discover_implicit_opportunities("REQ_001")
    print(f"Discovered {len(opportunities)} opportunities")
    
    for opp in opportunities:
        print(f"- {opp.description} (Value: ${opp.business_value}, Confidence: {opp.confidence_score:.2f})")
    
    # Get graph data for visualization
    graph_data = engine.get_opportunity_graph_data("REQ_001")
    print(f"Graph has {graph_data['stats']['total_nodes']} nodes and {graph_data['stats']['total_edges']} edges")
    
    # Get analytics data
    dashboard_data = engine.get_analytics_dashboard_data()
    print("Analytics data:", list(dashboard_data.keys()))
    
    # Run NetworkX analysis (minimal usage)
    bridge = NetworkXBridge(engine)
    centrality_scores = bridge.run_centrality_analysis("REQ_001")
    print(f"Centrality analysis completed for {len(centrality_scores)} nodes")
    
    engine.close()