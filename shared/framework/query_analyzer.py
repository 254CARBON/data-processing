"""Query analysis and optimization for ClickHouse."""

import json
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

import structlog

logger = structlog.get_logger()


class QueryType(str, Enum):
    """Query type enumeration."""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"


@dataclass
class QueryMetrics:
    """Query performance metrics."""
    query_id: str
    query_text: str
    query_type: QueryType
    execution_time: float
    rows_read: int
    rows_written: int
    memory_usage: int
    cpu_time: float
    timestamp: datetime
    user: str
    client_host: str
    exception: Optional[str] = None


@dataclass
class QueryAnalysis:
    """Query analysis results."""
    query_id: str
    query_text: str
    is_slow: bool
    performance_score: float
    recommendations: List[str]
    estimated_impact: str
    complexity_score: int


class ClickHouseQueryAnalyzer:
    """Analyzes ClickHouse query performance."""
    
    def __init__(self, clickhouse_client):
        self.clickhouse_client = clickhouse_client
        self.logger = structlog.get_logger("query-analyzer")
        self.slow_query_threshold = 1.0  # 1 second
        self.query_history: List[QueryMetrics] = []
        self.analysis_cache: Dict[str, QueryAnalysis] = {}
    
    async def analyze_query_log(self, hours: int = 24) -> List[QueryAnalysis]:
        """Analyze query log for the specified time period."""
        try:
            # Get query log from ClickHouse
            query_log = await self._get_query_log(hours)
            
            analyses = []
            for query_metrics in query_log:
                analysis = await self._analyze_query(query_metrics)
                analyses.append(analysis)
                
                # Cache analysis
                self.analysis_cache[query_metrics.query_id] = analysis
            
            self.logger.info("Query log analysis completed", 
                           hours=hours, 
                           queries_analyzed=len(analyses))
            
            return analyses
            
        except Exception as e:
            self.logger.error("Query log analysis failed", hours=hours, error=str(e))
            return []
    
    async def analyze_slow_queries(self, hours: int = 24) -> List[QueryAnalysis]:
        """Analyze slow queries from the query log."""
        try:
            all_analyses = await self.analyze_query_log(hours)
            slow_queries = [analysis for analysis in all_analyses if analysis.is_slow]
            
            self.logger.info("Slow query analysis completed", 
                           hours=hours, 
                           slow_queries=len(slow_queries))
            
            return slow_queries
            
        except Exception as e:
            self.logger.error("Slow query analysis failed", hours=hours, error=str(e))
            return []
    
    async def get_query_recommendations(self, query_text: str) -> List[str]:
        """Get optimization recommendations for a query."""
        try:
            recommendations = []
            
            # Analyze query structure
            if "SELECT *" in query_text.upper():
                recommendations.append("Avoid SELECT * - specify only needed columns")
            
            if "ORDER BY" in query_text.upper() and "LIMIT" not in query_text.upper():
                recommendations.append("Consider adding LIMIT to ORDER BY queries")
            
            if "JOIN" in query_text.upper():
                recommendations.append("Review JOIN conditions and indexes")
            
            if "GROUP BY" in query_text.upper():
                recommendations.append("Ensure GROUP BY columns are indexed")
            
            if "WHERE" in query_text.upper():
                recommendations.append("Review WHERE conditions and indexes")
            
            # Check for subqueries
            if "(" in query_text and "SELECT" in query_text.upper():
                recommendations.append("Consider optimizing subqueries")
            
            # Check for functions in WHERE clause
            if "WHERE" in query_text.upper() and any(func in query_text.upper() for func in ["DATE(", "YEAR(", "MONTH(", "DAY("]):
                recommendations.append("Avoid functions in WHERE clause - use date ranges instead")
            
            return recommendations
            
        except Exception as e:
            self.logger.error("Failed to get query recommendations", error=str(e))
            return []
    
    async def estimate_query_complexity(self, query_text: str) -> int:
        """Estimate query complexity score."""
        try:
            complexity = 0
            
            # Base complexity
            complexity += 1
            
            # Add complexity for various operations
            if "JOIN" in query_text.upper():
                complexity += 2
            if "GROUP BY" in query_text.upper():
                complexity += 2
            if "ORDER BY" in query_text.upper():
                complexity += 1
            if "HAVING" in query_text.upper():
                complexity += 2
            if "UNION" in query_text.upper():
                complexity += 3
            if "SUBQUERY" in query_text.upper() or "(" in query_text:
                complexity += 2
            
            # Add complexity for functions
            function_count = query_text.upper().count("(") - query_text.upper().count(")")
            complexity += min(function_count, 5)  # Cap function complexity
            
            return complexity
            
        except Exception as e:
            self.logger.error("Failed to estimate query complexity", error=str(e))
            return 1
    
    async def _get_query_log(self, hours: int) -> List[QueryMetrics]:
        """Get query log from ClickHouse."""
        try:
            # Query ClickHouse system.query_log table
            query = f"""
            SELECT 
                query_id,
                query,
                type,
                query_duration_ms / 1000.0 as execution_time,
                read_rows,
                written_rows,
                memory_usage,
                cpu_time_ms / 1000.0 as cpu_time,
                event_time,
                user,
                client_hostname,
                exception
            FROM system.query_log 
            WHERE event_time >= now() - INTERVAL {hours} HOUR
            AND type IN ('QueryStart', 'QueryFinish', 'ExceptionWhileProcessing')
            ORDER BY event_time DESC
            LIMIT 1000
            """
            
            results = await self.clickhouse_client.execute(query)
            
            query_metrics = []
            for row in results:
                metrics = QueryMetrics(
                    query_id=row.get("query_id", ""),
                    query_text=row.get("query", ""),
                    query_type=QueryType(row.get("type", "SELECT")),
                    execution_time=row.get("execution_time", 0.0),
                    rows_read=row.get("read_rows", 0),
                    rows_written=row.get("written_rows", 0),
                    memory_usage=row.get("memory_usage", 0),
                    cpu_time=row.get("cpu_time", 0.0),
                    timestamp=row.get("event_time", datetime.utcnow()),
                    user=row.get("user", ""),
                    client_host=row.get("client_hostname", ""),
                    exception=row.get("exception")
                )
                query_metrics.append(metrics)
            
            return query_metrics
            
        except Exception as e:
            self.logger.error("Failed to get query log", hours=hours, error=str(e))
            return []
    
    async def _analyze_query(self, query_metrics: QueryMetrics) -> QueryAnalysis:
        """Analyze a single query."""
        try:
            # Determine if query is slow
            is_slow = query_metrics.execution_time > self.slow_query_threshold
            
            # Calculate performance score (0-100)
            performance_score = self._calculate_performance_score(query_metrics)
            
            # Get recommendations
            recommendations = await self.get_query_recommendations(query_metrics.query_text)
            
            # Estimate impact
            estimated_impact = self._estimate_impact(query_metrics)
            
            # Calculate complexity score
            complexity_score = await self.estimate_query_complexity(query_metrics.query_text)
            
            analysis = QueryAnalysis(
                query_id=query_metrics.query_id,
                query_text=query_metrics.query_text,
                is_slow=is_slow,
                performance_score=performance_score,
                recommendations=recommendations,
                estimated_impact=estimated_impact,
                complexity_score=complexity_score
            )
            
            return analysis
            
        except Exception as e:
            self.logger.error("Failed to analyze query", 
                            query_id=query_metrics.query_id, 
                            error=str(e))
            return QueryAnalysis(
                query_id=query_metrics.query_id,
                query_text=query_metrics.query_text,
                is_slow=False,
                performance_score=0,
                recommendations=[],
                estimated_impact="unknown",
                complexity_score=1
            )
    
    def _calculate_performance_score(self, query_metrics: QueryMetrics) -> float:
        """Calculate performance score for a query."""
        try:
            score = 100.0
            
            # Penalize slow execution time
            if query_metrics.execution_time > 0:
                time_penalty = min(query_metrics.execution_time * 10, 50)
                score -= time_penalty
            
            # Penalize high memory usage
            if query_metrics.memory_usage > 0:
                memory_penalty = min(query_metrics.memory_usage / 1024 / 1024, 30)
                score -= memory_penalty
            
            # Penalize high CPU time
            if query_metrics.cpu_time > 0:
                cpu_penalty = min(query_metrics.cpu_time * 5, 20)
                score -= cpu_penalty
            
            # Ensure score is between 0 and 100
            return max(0, min(100, score))
            
        except Exception as e:
            self.logger.error("Failed to calculate performance score", error=str(e))
            return 0
    
    def _estimate_impact(self, query_metrics: QueryMetrics) -> str:
        """Estimate the impact of optimizing a query."""
        try:
            if query_metrics.execution_time < 0.1:
                return "low"
            elif query_metrics.execution_time < 1.0:
                return "medium"
            elif query_metrics.execution_time < 10.0:
                return "high"
            else:
                return "critical"
                
        except Exception as e:
            self.logger.error("Failed to estimate impact", error=str(e))
            return "unknown"


class QueryOptimizer:
    """Provides query optimization suggestions."""
    
    def __init__(self, clickhouse_client):
        self.clickhouse_client = clickhouse_client
        self.logger = structlog.get_logger("query-optimizer")
    
    async def suggest_indexes(self, query_text: str) -> List[str]:
        """Suggest indexes for a query."""
        try:
            suggestions = []
            
            # Analyze WHERE conditions
            if "WHERE" in query_text.upper():
                # Extract WHERE conditions (simplified)
                where_clause = query_text.upper().split("WHERE")[1].split("ORDER BY")[0].split("GROUP BY")[0]
                
                # Look for column comparisons
                if "=" in where_clause:
                    suggestions.append("Consider adding index on columns used in equality comparisons")
                
                if ">" in where_clause or "<" in where_clause:
                    suggestions.append("Consider adding index on columns used in range comparisons")
                
                if "IN" in where_clause:
                    suggestions.append("Consider adding index on columns used in IN clauses")
            
            # Analyze JOIN conditions
            if "JOIN" in query_text.upper():
                suggestions.append("Ensure JOIN columns are indexed")
            
            # Analyze ORDER BY
            if "ORDER BY" in query_text.upper():
                suggestions.append("Consider adding index on ORDER BY columns")
            
            # Analyze GROUP BY
            if "GROUP BY" in query_text.upper():
                suggestions.append("Consider adding index on GROUP BY columns")
            
            return suggestions
            
        except Exception as e:
            self.logger.error("Failed to suggest indexes", error=str(e))
            return []
    
    async def suggest_query_rewrite(self, query_text: str) -> List[str]:
        """Suggest query rewrites for optimization."""
        try:
            suggestions = []
            
            # Check for SELECT *
            if "SELECT *" in query_text.upper():
                suggestions.append("Replace SELECT * with specific columns")
            
            # Check for unnecessary ORDER BY
            if "ORDER BY" in query_text.upper() and "LIMIT" not in query_text.upper():
                suggestions.append("Add LIMIT to ORDER BY queries")
            
            # Check for subqueries that could be JOINs
            if "(" in query_text and "SELECT" in query_text.upper():
                suggestions.append("Consider converting subqueries to JOINs")
            
            # Check for functions in WHERE clause
            if "WHERE" in query_text.upper():
                if any(func in query_text.upper() for func in ["DATE(", "YEAR(", "MONTH(", "DAY("]):
                    suggestions.append("Move date functions to application layer")
            
            return suggestions
            
        except Exception as e:
            self.logger.error("Failed to suggest query rewrite", error=str(e))
            return []
    
    async def analyze_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Analyze table statistics for optimization."""
        try:
            # Get table information
            table_info_query = f"""
            SELECT 
                name,
                engine,
                total_rows,
                total_bytes,
                compression_ratio
            FROM system.tables 
            WHERE name = '{table_name}'
            """
            
            results = await self.clickhouse_client.execute(table_info_query)
            if not results:
                return {}
            
            table_info = results[0]
            
            # Get column information
            columns_query = f"""
            SELECT 
                name,
                type,
                default_kind,
                default_expression
            FROM system.columns 
            WHERE table = '{table_name}'
            ORDER BY position
            """
            
            columns = await self.clickhouse_client.execute(columns_query)
            
            return {
                "table_name": table_info.get("name"),
                "engine": table_info.get("engine"),
                "total_rows": table_info.get("total_rows", 0),
                "total_bytes": table_info.get("total_bytes", 0),
                "compression_ratio": table_info.get("compression_ratio", 0),
                "columns": columns
            }
            
        except Exception as e:
            self.logger.error("Failed to analyze table stats", 
                            table_name=table_name, 
                            error=str(e))
            return {}


class QueryPerformanceMonitor:
    """Monitors query performance and provides insights."""
    
    def __init__(self, query_analyzer: ClickHouseQueryAnalyzer):
        self.query_analyzer = query_analyzer
        self.logger = structlog.get_logger("query-performance-monitor")
        self.performance_history: Dict[str, List[float]] = {}
    
    async def get_performance_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get query performance summary."""
        try:
            analyses = await self.query_analyzer.analyze_query_log(hours)
            
            if not analyses:
                return {"error": "No queries found"}
            
            # Calculate summary statistics
            total_queries = len(analyses)
            slow_queries = len([a for a in analyses if a.is_slow])
            avg_performance_score = sum(a.performance_score for a in analyses) / total_queries
            
            # Group by query type
            query_types = {}
            for analysis in analyses:
                query_type = "SELECT"  # Simplified
                if query_type not in query_types:
                    query_types[query_type] = []
                query_types[query_type].append(analysis)
            
            # Get top slow queries
            top_slow_queries = sorted(analyses, key=lambda x: x.performance_score)[:10]
            
            return {
                "total_queries": total_queries,
                "slow_queries": slow_queries,
                "slow_query_percentage": (slow_queries / total_queries * 100) if total_queries > 0 else 0,
                "avg_performance_score": avg_performance_score,
                "query_types": {k: len(v) for k, v in query_types.items()},
                "top_slow_queries": [
                    {
                        "query_id": q.query_id,
                        "performance_score": q.performance_score,
                        "recommendations": q.recommendations[:3]  # Top 3 recommendations
                    }
                    for q in top_slow_queries
                ],
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error("Failed to get performance summary", hours=hours, error=str(e))
            return {"error": str(e)}
    
    async def detect_performance_issues(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Detect query performance issues."""
        try:
            analyses = await self.query_analyzer.analyze_query_log(hours)
            issues = []
            
            for analysis in analyses:
                if analysis.performance_score < 50:
                    issues.append({
                        "type": "slow_query",
                        "severity": "warning",
                        "query_id": analysis.query_id,
                        "performance_score": analysis.performance_score,
                        "recommendations": analysis.recommendations,
                        "impact": analysis.estimated_impact
                    })
                
                if analysis.complexity_score > 10:
                    issues.append({
                        "type": "complex_query",
                        "severity": "info",
                        "query_id": analysis.query_id,
                        "complexity_score": analysis.complexity_score,
                        "recommendations": ["Consider breaking down complex queries"]
                    })
            
            return issues
            
        except Exception as e:
            self.logger.error("Failed to detect performance issues", hours=hours, error=str(e))
            return []
