
"""
FastAPI application for Google Ads Offline Conversions
"""
from fastapi import FastAPI, HTTPException, Query, Request, Depends, Form
from fastapi.responses import Response, HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
from typing import Optional
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from config import settings
from models import PostbackRequest, ConversionResponse, CleanupResponse
from csv_handler import CSVHandler
from auth import authenticate_dashboard, get_security_stats

# Initialize FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description="Sistema para receber postbacks e gerar CSVs para Google Ads",
    version="1.0.0"
)

# Initialize templates
templates = Jinja2Templates(directory="templates")

# Initialize CSV handler
csv_handler = CSVHandler()

# Initialize scheduler
scheduler = BackgroundScheduler(timezone=pytz.timezone('America/Sao_Paulo'))


@app.on_event("startup")
async def startup_event():
    """Start scheduler when application starts"""
    # Execute cleanup every day at 01:30 (GMT-03:00)
    scheduler.add_job(
        run_cleanup,
        CronTrigger(hour=1, minute=30, timezone='America/Sao_Paulo'),
        id='daily_cleanup',
        name='Limpeza diária de conversões antigas',
        replace_existing=True
    )
    scheduler.start()
    print("✅ Scheduler iniciado - Limpeza automática configurada para 01:30 (GMT-03:00)")


@app.on_event("shutdown")
async def shutdown_event():
    """Stop scheduler when application shuts down"""
    scheduler.shutdown()
    print("🛑 Scheduler encerrado")


def run_cleanup():
    """Function executed by scheduler for automatic cleanup"""
    print(f"🧹 Iniciando limpeza automática - {datetime.now()}")
    try:
        results = csv_handler.cleanup_all_sources(hours=25)
        
        total_archived = sum(r['archived'] for r in results.values())
        total_remaining = sum(r['remaining'] for r in results.values())
        
        print(f"✅ Limpeza concluída - Arquivadas: {total_archived}, Restantes: {total_remaining}")
        print(f"📊 Detalhes: {results}")
    except Exception as e:
        print(f"❌ Erro na limpeza automática: {e}")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, username: str = Depends(authenticate_dashboard)):
    """Dashboard de monitoramento com autenticação segura"""
    try:
        # Get all customer IDs
        sources = csv_handler.get_all_sources()
        
        # Get conversion counts
        stats = []
        total_conversions = 0
        total_recent = 0
        total_history = 0
        
        for src in sources:
            # Skip history files
            if src.endswith('_history'):
                continue
                
            counts = csv_handler.get_conversion_count(src)
            total_conversions += counts['total']
            total_recent += counts['recent']
            total_history += counts['history']
            csv_url = csv_handler.get_csv_url(src)
            history_url = csv_handler.get_csv_url(f"{src}_history")
            stats.append({
                'src': src,
                'recent_count': counts['recent'],
                'history_count': counts['history'],
                'total_count': counts['total'],
                'csv_url': csv_url,
                'history_url': history_url
            })
        
        # Log successful dashboard access
        print(f"✅ Dashboard acessado por usuário autenticado: {username}")
        
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "total_conversions": total_conversions,
                "total_recent": total_recent,
                "total_history": total_history,
                "total_accounts": len(stats),
                "stats": stats,
                "app_name": settings.APP_NAME,
                "authenticated_user": username
            }
        )\r
    except Exception as e:\r
        print(f"❌ Error loading dashboard: {e}")\r
        return HTMLResponse(\r
            content=f"<h1>Error loading dashboard</h1><p>{str(e)}</p>",\r
            status_code=500\r
        )\r
\r
\r
@app.post("/add-source")\r
async def add_source(request: Request, src: str = Form(...), username: str = Depends(authenticate_dashboard)):\r
    """Adiciona uma nova conta (src) criando um CSV vazio se não existir."""\r
    try:\r
        created = csv_handler.create_empty_source(src)\r
        if created:\r
            msg = f"Conta '{src}' adicionada com sucesso."\r
        else:\r
            msg = f"Erro ao adicionar conta '{src}'."\r
        \r
        # Recarrega dashboard com mensagem\r
        sources = csv_handler.get_all_sources()\r
        stats = []\r
        total_conversions = 0\r
        total_recent = 0\r
        total_history = 0\r
        \r
        for s in sources:\r
            # Skip history files\r
            if s.endswith('_history'):\r
                continue\r
                \r
            counts = csv_handler.get_conversion_count(s)\r
            total_conversions += counts['total']\r
            total_recent += counts['recent']\r
            total_history += counts['history']\r
            csv_url = csv_handler.get_csv_url(s)\r
            history_url = csv_handler.get_csv_url(f"{s}_history")\r
            stats.append({\r
                'src': s,\r
                'recent_count': counts['recent'],\r
                'history_count': counts['history'],\r
                'total_count': counts['total'],\r
                'csv_url': csv_url,\r
                'history_url': history_url\r
            })\r
        \r
        return templates.TemplateResponse(\r
            "index.html",\r
            {\r
                "request": request,\r
                "total_conversions": total_conversions,\r
                "total_recent": total_recent,\r
                "total_history": total_history,\r
                "total_accounts": len(stats),\r
                "stats": stats,\r
                "app_name": settings.APP_NAME,\r
                "authenticated_user": username,\r
                "add_source_msg": msg\r
            }\r
        )\r
    except Exception as e:\r
        print(f"❌ Erro ao adicionar conta: {e}")\r
        return HTMLResponse(\r
            content=f"<h1>Erro ao adicionar conta</h1><p>{str(e)}</p>",\r
            status_code=500\r
        )\r
\r
\r
@app.get("/postback")
@app.post("/postback")
async def receive_postback(
    gclid: str = Query(..., description="Google Click ID"),
    ctid: str = Query(..., description="Customer ID do Google Ads"),
    orderId: Optional[str] = Query(None),
    commission: Optional[float] = Query(None),
    productName: Optional[str] = Query(None),
    productId: Optional[str] = Query(None),
    dateTime: Optional[str] = Query(None),
    utmSource: Optional[str] = Query(None),
    utmCampaign: Optional[str] = Query(None),
    utmMedium: Optional[str] = Query(None),
    utmContent: Optional[str] = Query(None),
    utmTerm: Optional[str] = Query(None),
    upsellNo: Optional[int] = Query(None),
):
    """
    Recebe postback de conversão e adiciona ao CSV correspondente
    
    Parâmetros obrigatórios:
    - gclid: Google Click ID
    - ctid: Customer ID do Google Ads
    
    Parâmetros opcionais:
    - commission: Valor da comissão
    - dateTime: Data/hora da conversão (ISO 8601)
    - orderId, productName, productId, utmSource, etc.
    """
    try:
        # Validate request using Pydantic model
        postback = PostbackRequest(
            gclid=gclid,
            ctid=ctid,
            orderId=orderId,
            commission=commission,
            productName=productName,
            productId=productId,
            dateTime=dateTime,
            utmSource=utmSource,
            utmCampaign=utmCampaign,
            utmMedium=utmMedium,
            utmContent=utmContent,
            utmTerm=utmTerm,
            upsellNo=upsellNo
        )

        # Só aceitar vendas vindas do Google
        if not postback.utmSource or postback.utmSource.lower() != 'google':
            raise HTTPException(status_code=400, detail="Conversão rejeitada: utm_source diferente de 'google'.")
        # (Opcional) Validar utm_medium e utm_campaign se quiser mais restrição
        # if not postback.utmMedium or postback.utmMedium.lower() != 'cpc':
        #     raise HTTPException(status_code=400, detail="Conversão rejeitada: utm_medium diferente de 'cpc'.")
        # if not postback.utmCampaign:
        #     raise HTTPException(status_code=400, detail="Conversão rejeitada: utm_campaign não informado.")

        # Use provided datetime or current time
        conversion_time = postback.dateTime if postback.dateTime else datetime.utcnow().isoformat()

        # Add conversion to CSV
        success = csv_handler.add_conversion(
            ctid=postback.ctid,
            gclid=postback.gclid,
            conversion_time=conversion_time,
            conversion_value=postback.commission
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="Erro ao salvar conversão")
        
        # Get CSV URL
        csv_url = csv_handler.get_csv_url(postback.ctid)
        
        # Log success
        print(f"✅ Conversão recebida - CTID: {postback.ctid}, GCLID: {postback.gclid}, Valor: {postback.commission}")
        
        # Get conversion counts
        counts = csv_handler.get_conversion_count(postback.ctid)
        
        return ConversionResponse(
            success=True,
            message=f"Conversão registrada com sucesso! Total de conversões para conta {postback.ctid}: {counts['total']}",
            ctid=postback.ctid,
            gclid=postback.gclid,
            csv_url=csv_url
        )
        
    except ValueError as e:
        print(f"❌ Erro de validação: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"❌ Erro ao processar postback: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.get("/csv/{api_key}/{ctid}.csv")
async def get_csv(
    api_key: str,
    ctid: str
):
    """
    Retorna o CSV para uma conta específica
    URL termina em .csv para compatibilidade com Google Ads
    
    Parâmetros:
    - api_key: Chave de API para autenticação (no path)
    - ctid: Customer ID do Google Ads
    
    Exemplo: /csv/sua-api-key/7871141994.csv
    """
    # Validate API key
    if api_key != settings.API_KEY:
        print(f"❌ Tentativa de acesso não autorizado ao CSV {ctid} com API key inválida")
        raise HTTPException(status_code=401, detail="API Key inválida")
    
    # Get CSV content
    csv_content = csv_handler.get_csv_content(ctid)
    
    if csv_content is None:
        raise HTTPException(status_code=404, detail=f"CSV não encontrado para conta {ctid}")
    
    print(f"📥 CSV acessado com sucesso - CTID: {ctid}")
    
    # Return CSV as downloadable file
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={ctid}.csv"
        }
    )


@app.post("/cleanup/{src}")
async def manual_cleanup(
    src: str,
    request: Request,
    hours: int = Query(25, description="Number of hours threshold"),
    username: str = Depends(authenticate_dashboard)
) -> CleanupResponse:
    """
    Execute manual cleanup of old conversions
    Useful for testing or on-demand cleanup
    
    Args:
        src: Customer ID
        hours: Number of hours threshold (default: 25)
    """
    try:
        results = csv_handler.cleanup_old_conversions(src, hours)
        
        print(f"🧹 Limpeza manual executada por {username} - Conta: {src}")
        
        return CleanupResponse(
            success=True,
            src=src,
            archived=results['archived'],
            remaining=results['remaining'],
            message=f"Arquivadas {results['archived']} conversões antigas"
        )
    except Exception as e:
        print(f"❌ Erro na limpeza manual: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao executar limpeza: {str(e)}")


@app.get("/csv/{api_key}/{ctid}_history.csv")
async def get_history_csv(
    api_key: str,
    ctid: str
):
    """
    Return history CSV for a specific account
    For audit purposes only, not used by Google Ads
    
    Args:
        api_key: API key for authentication (in path)
        ctid: Customer ID
    
    Example: /csv/your-api-key/7871141994_history.csv
    """
    # Validate API key
    if api_key != settings.API_KEY:
        print(f"❌ Tentativa de acesso não autorizado ao histórico {ctid} com API key inválida")
        raise HTTPException(status_code=401, detail="API Key inválida")
    
    # Get history CSV content
    csv_content = csv_handler.get_csv_content(f"{ctid}_history")
    
    if csv_content is None:
        raise HTTPException(status_code=404, detail=f"Histórico não encontrado para conta {ctid}")
    
    print(f"📜 Histórico acessado com sucesso - CTID: {ctid}")
    
    # Return CSV as downloadable file
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={ctid}_history.csv"
        }
    )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": settings.APP_NAME,
        "scheduler_running": scheduler.running
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )
