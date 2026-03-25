"""src/prompts — LLM prompt templates for all agents."""
from src.prompts.credit_analysis import CREDIT_ANALYSIS_SYSTEM, build_credit_analysis_user
from src.prompts.fraud_detection import FRAUD_DETECTION_SYSTEM, build_fraud_detection_user
from src.prompts.decision_orchestrator import DECISION_ORCHESTRATOR_SYSTEM, build_decision_orchestrator_user
from src.prompts.document_extraction import build_extraction_system, build_extraction_user
from src.prompts.document_quality import DOCUMENT_QUALITY_SYSTEM, build_document_quality_user

__all__ = [
    "CREDIT_ANALYSIS_SYSTEM",
    "build_credit_analysis_user",
    "FRAUD_DETECTION_SYSTEM",
    "build_fraud_detection_user",
    "DECISION_ORCHESTRATOR_SYSTEM",
    "build_decision_orchestrator_user",
    "build_extraction_system",
    "build_extraction_user",
    "DOCUMENT_QUALITY_SYSTEM",
    "build_document_quality_user",
]
