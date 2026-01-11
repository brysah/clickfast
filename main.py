"""
FastAPI application for Google Ads Offline Conversions
"""
from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.responses import Response, HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
from typing import Optional
import uvicorn

from config import settings
from models import PostbackRequest, ConversionResponse
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


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, username: str = Depends(authenticate_dashboard)):
    """Dashboard de monitoramento com autenticação segura"""
    try:
        # Get all customer IDs
        sources = csv_handler.get_all_sources()
        
        # Get conversion counts
        stats = []
        total_conversions = 0
        
        for src in sources:
            count = csv_handler.get_conversion_count(src)
            total_conversions += count
            csv_url = csv_handler.get_csv_url(src)
            stats.append({
                'src': src,
                'count': count,
                'csv_url': csv_url
            })
        
        # Log successful dashboard access
        print(f"✅ Dashboard acessado por usuário autenticado: {username}")
        
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "total_conversions": total_conversions,
                "total_accounts": len(sources),
                "stats": stats,
                "app_name": settings.APP_NAME,
                "authenticated_user": username
            }
        )
    except Exception as e:
        print(f"❌ Error loading dashboard: {e}")
        return HTMLResponse(
            content=f"<h1>Error loading dashboard</h1><p>{str(e)}</p>",
            status_code=500
        )


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
        
        return ConversionResponse(
            success=True,
            message=f"Conversão registrada com sucesso! Total de conversões para conta {postback.ctid}: {csv_handler.get_conversion_count(postback.ctid)}",
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


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": settings.APP_NAME
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )
