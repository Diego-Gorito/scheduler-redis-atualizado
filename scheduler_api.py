import os
import json
import redis
import logging
import requests
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
import pytz

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Carrega variáveis de ambiente
load_dotenv()

# Configurações
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
API_TOKEN = os.getenv("API_TOKEN", "your-secret-token-here")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Inicializa FastAPI
app = FastAPI(title="Scheduler API", version="1.2.0")
security = HTTPBearer()

# Inicializa Redis
redis_client = redis.Redis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    db=0, 
    decode_responses=True
)

# Inicializa APScheduler
scheduler = BackgroundScheduler(timezone=pytz.UTC)
scheduler.start()
logger.info("APScheduler iniciado")

# Modelos Pydantic
class ScheduleMessage(BaseModel):
    id: str
    scheduleTo: str
    payload: dict
    webhookUrl: str

class MessageResponse(BaseModel):
    status: str
    messageId: str

# Autenticação
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido"
        )
    return credentials.credentials

# Função para enviar webhook
def send_webhook(message_id: str, payload: dict, webhook_url: str):
    """Envia o webhook e remove a mensagem do Redis"""
    try:
        logger.info(f"Enviando webhook para {webhook_url} com messageId: {message_id}")
        
        # Envia o webhook
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        logger.info(f"Webhook enviado. Status: {response.status_code}")
        
        # Remove a mensagem do Redis após envio bem-sucedido
        redis_client.delete(f"message:{message_id}")
        logger.info(f"Mensagem {message_id} removida do Redis")
        
    except Exception as e:
        logger.error(f"Erro ao enviar webhook para {webhook_url}: {str(e)}")
        # Mantém a mensagem no Redis em caso de erro
        redis_client.setex(
            f"message:{message_id}:error",
            3600,  # 1 hora
            str(e)
        )

# Função para agendar mensagem
def schedule_message_job(message_id: str, schedule_to: str, payload: dict, webhook_url: str):
    """Agenda uma mensagem usando APScheduler"""
    try:
        # Parse da data
        schedule_datetime = datetime.fromisoformat(schedule_to.replace('Z', '+00:00'))
        
        # Cria o trigger de data
        trigger = DateTrigger(run_date=schedule_datetime)
        
        # Adiciona o job ao scheduler
        job = scheduler.add_job(
            send_webhook,
            trigger=trigger,
            args=[message_id, payload, webhook_url],
            id=message_id,
            replace_existing=True  # IMPORTANTE: substitui job existente com mesmo ID
        )
        
        logger.info(f"Job agendado: {message_id} para {schedule_datetime}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao agendar mensagem {message_id}: {str(e)}")
        return False

# Função para restaurar mensagens ao iniciar
def restore_messages_from_redis():
    """Restaura todas as mensagens do Redis ao iniciar o servidor"""
    try:
        keys = redis_client.keys("message:*")
        
        # Filtra apenas as chaves de mensagens (não as de erro)
        message_keys = [k for k in keys if not k.endswith(":error")]
        
        logger.info(f"Restaurando {len(message_keys)} mensagens do Redis")
        
        for key in message_keys:
            try:
                message_data = redis_client.get(key)
                if message_data:
                    message = json.loads(message_data)
                    schedule_message_job(
                        message["id"],
                        message["scheduleTo"],
                        message["payload"],
                        message["webhookUrl"]
                    )
            except Exception as e:
                logger.error(f"Erro ao restaurar mensagem {key}: {str(e)}")
        
        logger.info("Restauração de mensagens concluída")
        
    except Exception as e:
        logger.error(f"Erro ao restaurar mensagens: {str(e)}")

# Endpoints da API
@app.on_event("startup")
async def startup_event():
    """Restaura mensagens ao iniciar"""
    restore_messages_from_redis()

@app.get("/health")
async def health_check():
    """Verifica a saúde do serviço"""
    try:
        redis_client.ping()
        redis_status = "connected"
    except:
        redis_status = "disconnected"
    
    return {
        "status": "healthy",
        "redis": redis_status,
        "scheduler": "running" if scheduler.running else "stopped",
        "scheduled_jobs": len(scheduler.get_jobs())
    }

@app.post("/messages", response_model=MessageResponse)
async def create_message(
    message: ScheduleMessage,
    token: str = Depends(verify_token)
):
    """
    Cria ou atualiza uma mensagem agendada.
    Se o ID já existir, remove o agendamento anterior e cria um novo.
    """
    try:
        message_key = f"message:{message.id}"
        
        # Verifica se a mensagem já existe
        existing_message = redis_client.get(message_key)
        
        if existing_message:
            logger.info(f"Mensagem {message.id} já existe. Atualizando...")
            
            # Remove o job antigo do scheduler
            try:
                scheduler.remove_job(message.id)
                logger.info(f"Job antigo {message.id} removido do scheduler")
            except Exception as e:
                logger.warning(f"Job {message.id} não encontrado no scheduler: {str(e)}")
            
            # Remove a mensagem antiga do Redis
            redis_client.delete(message_key)
            logger.info(f"Mensagem antiga {message.id} removida do Redis")
        
        # Salva a nova mensagem no Redis
        message_data = message.dict()
        redis_client.set(message_key, json.dumps(message_data))
        logger.info(f"Nova mensagem {message.id} salva no Redis")
        
        # Agenda o novo job
        success = schedule_message_job(
            message.id,
            message.scheduleTo,
            message.payload,
            message.webhookUrl
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Erro ao agendar mensagem"
            )
        
        status_msg = "updated" if existing_message else "scheduled"
        
        return MessageResponse(
            status=status_msg,
            messageId=message.id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.get("/messages")
async def list_messages(token: str = Depends(verify_token)):
    """Lista todas as mensagens agendadas"""
    try:
        jobs = scheduler.get_jobs()
        
        scheduled_jobs = []
        for job in jobs:
            scheduled_jobs.append({
                "messageId": job.id,
                "nextRun": str(job.next_run_time),
                "trigger": str(job.trigger)
            })
        
        return {
            "scheduledJobs": scheduled_jobs,
            "count": len(scheduled_jobs)
        }
        
    except Exception as e:
        logger.error(f"Erro ao listar mensagens: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.delete("/messages/{message_id}", response_model=MessageResponse)
async def delete_message(
    message_id: str,
    token: str = Depends(verify_token)
):
    """Remove uma mensagem agendada"""
    try:
        message_key = f"message:{message_id}"
        
        # Verifica se a mensagem existe no Redis
        if not redis_client.exists(message_key):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Mensagem {message_id} não encontrada"
            )
        
        # Remove do scheduler
        try:
            scheduler.remove_job(message_id)
            logger.info(f"Job {message_id} removido do scheduler")
        except Exception as e:
            logger.warning(f"Job {message_id} não encontrado no scheduler: {str(e)}")
        
        # Remove do Redis
        redis_client.delete(message_key)
        logger.info(f"Mensagem {message_id} removida do Redis")
        
        return MessageResponse(
            status="deleted",
            messageId=message_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar mensagem: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# Execução
if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Iniciando servidor em {API_HOST}:{API_PORT}")
    uvicorn.run(app, host=API_HOST, port=API_PORT)
