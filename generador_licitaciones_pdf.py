from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from datetime import datetime
import locale

# Configurar locale para formato de moneda
try:
    locale.setlocale(locale.LC_ALL, 'es_CL.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'Spanish_Chile.1252')
    except:
        pass

def formatear_monto(valor):
    """Formatea valores monetarios al estilo chileno sin añadir ceros extra."""
    try:
        if valor is None or str(valor).strip() == '': return '$0'
        valor_num = float(valor)
        return f"${valor_num:,.0f}".replace(',', '.')
    except (ValueError, TypeError):
        return '$0'

def formatear_fecha(fecha_str):
    if not fecha_str or fecha_str == '': return 'N/A'
    try:
        fecha_limpia = str(fecha_str).split(' ')[0]
        for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%d/%m/%Y'):
            try:
                return datetime.strptime(fecha_limpia, fmt).strftime('%d-%m-%Y')
            except ValueError:
                continue
        return fecha_limpia
    except:
        return str(fecha_str)

def generar_pdf_licitacion(licitacion_data, nombre_archivo):
    doc = SimpleDocTemplate(nombre_archivo, pagesize=letter, rightMargin=0.75*inch, leftMargin=0.75*inch, topMargin=0.75*inch, bottomMargin=0.75*inch)
    styles = getSampleStyleSheet()
    elementos = []

    # Estilos de Párrafo personalizados
    style_label = ParagraphStyle(name='Label', fontName='Helvetica-Bold', fontSize=9, alignment=TA_LEFT)
    style_value = ParagraphStyle(name='Value', fontName='Helvetica', fontSize=9, alignment=TA_LEFT)
    titulo_style = ParagraphStyle(name='Title', fontSize=14, fontName='Helvetica-Bold', alignment=TA_CENTER, spaceAfter=20)
    subtitulo_style = ParagraphStyle(name='SubTitle', fontSize=11, fontName='Helvetica-Bold', alignment=TA_CENTER, spaceAfter=12)

    num_proveedores = len(licitacion_data.get('convenios', []))

    for i, convenio in enumerate(licitacion_data.get('convenios', [])):
        # --- PÁGINA 1: FICHA DE CONVENIO ---
        elementos.append(Paragraph('FICHA CONVENIO SUMINISTRO', titulo_style))

        # --- Tabla de Información General (sin grid) ---
        info_general_data = [
            [Paragraph('ID', style_label), Paragraph(str(licitacion_data.get('id_licitacion', 'N/A')), style_value)],
            [Paragraph('ID GESTION DE CONTRATOS', style_label), Paragraph(str(convenio.get('id_gestion_contratos', 'VIGENTE')), style_value)],
            [Paragraph('CONVENIO SUMINISTRO', style_label), Paragraph(str(licitacion_data.get('nombre_licitacion', 'N/A')), style_value)],
            [Paragraph('DECRETO ADJUDICACIÓN', style_label), Paragraph(str(licitacion_data.get('decreto_adjudicacion', 'N/A')), style_value)],
            [Paragraph('MONTO LICITADO', style_label), Paragraph(formatear_monto(convenio.get('monto_adjudicado', 0)), style_value)],
            [Paragraph('INSPECTOR TECNICO', style_label), Paragraph(str(licitacion_data.get('inspector_tecnico', 'N/A')), style_value)],
            [Paragraph('N° PROVEEDORES ADJUDICADOS', style_label), Paragraph(str(num_proveedores), style_value)],
        ]
        info_general_table = Table(info_general_data, colWidths=[2.5*inch, 4.5*inch])
        info_general_table.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LINEBELOW', (0,0), (-1,-2), 0.5, colors.lightgrey),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ]))
        elementos.append(info_general_table)
        elementos.append(Spacer(1, 0.3*inch))
        
        # --- Título Adjudicatario ---
        titulo_adj_data = [[Paragraph(f'ADJUDICATARIO N°{i+1}', ParagraphStyle(name='AdjTitle', fontName='Helvetica-Bold', fontSize=10, alignment=TA_CENTER, textColor=colors.black))]]
        titulo_adj_table = Table(titulo_adj_data, colWidths=[7*inch])
        titulo_adj_table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#EFEFEF')),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('TOPPADDING', (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ]))
        elementos.append(titulo_adj_table)
        
        # --- Tabla de Adjudicatario (sin grid) ---
        adjudicatario_data = [
            [Paragraph('PROVEEDOR', style_label), Paragraph(str(convenio.get('proveedor', 'N/A')), style_value)],
            [Paragraph('RUT', style_label), Paragraph(str(convenio.get('rut_proveedor', 'N/A')), style_value)],
            [Paragraph('DIRECCIÓN', style_label), Paragraph(str(convenio.get('direccion_proveedor', 'N/A')), style_value)],
            [Paragraph('TELÉFONO', style_label), Paragraph(str(convenio.get('telefono_proveedor', 'N/A')), style_value)],
            [Paragraph('CORREO', style_label), Paragraph(str(convenio.get('correo_proveedor', 'N/A')), style_value)],
            [Paragraph('PLAZO CONTRACTUAL', style_label), Paragraph(f"{convenio.get('meses', 'N/A')} MESES", style_value)],
            [Paragraph('INICIO Formalización', style_label), Paragraph(str(convenio.get('inicio_contrato', 'Fecha decreto autoriza contrato')), style_value)],
            [Paragraph('FECHA INICIO', style_label), Paragraph(formatear_fecha(convenio.get('fecha_inicio')), style_value)],
            [Paragraph('FECHA TÉRMINO', style_label), Paragraph(formatear_fecha(convenio.get('fecha_termino')), style_value)],
            [Paragraph('TIENE ACTUALIZACIÓN POR IPC', style_label), Paragraph(str(convenio.get('tiene_ipc', 'SI')), style_value)],
            [Paragraph('GARANTÍA', style_label), Paragraph(str(convenio.get('garantia', 'N/A')), style_value)],
            [Paragraph('CONTRATO', style_label), Paragraph('SI', style_value)],
            [Paragraph('DECRETO APRUEBA CONTRATO', style_label), Paragraph(str(convenio.get('decreto_aprueba_contrato', 'N/A')), style_value)],
        ]
        adjudicatario_table = Table(adjudicatario_data, colWidths=[2.5*inch, 4.5*inch])
        adjudicatario_table.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LINEBELOW', (0,0), (-1,-2), 0.5, colors.lightgrey),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ]))
        elementos.append(adjudicatario_table)
        elementos.append(PageBreak())

        # --- PÁGINA 2: DETALLE DE OCs ---
        elementos.append(Paragraph('DETALLE CONVENIO SUMINISTRO', subtitulo_style))
        elementos.append(Spacer(1, 0.1*inch))
        
        detalle_convenio_data = [
            [Paragraph(f"<b>PROVEEDOR:</b> {str(convenio.get('proveedor', 'N/A'))}", style_value)],
            [Paragraph(f"<b>RUT:</b> {str(convenio.get('rut_proveedor', 'N/A'))}", style_value)],
            [Paragraph(f"<b>FECHA DE INICIO:</b> {formatear_fecha(convenio.get('fecha_inicio'))}", style_value)],
            [Paragraph(f"<b>FECHA DE TÉRMINO:</b> {formatear_fecha(convenio.get('fecha_termino'))}", style_value)],
            [Paragraph(f"<b>PRESUPUESTO ESTIMADO:</b> {formatear_monto(convenio.get('monto_adjudicado', 0))}", style_value)],
        ]
        detalle_convenio_table = Table(detalle_convenio_data, colWidths=[7*inch])
        detalle_convenio_table.setStyle(TableStyle([
            ('BOX', (0,0), (-1,-1), 1, colors.black),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('TOPPADDING', (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ]))
        elementos.append(detalle_convenio_table)
        elementos.append(Spacer(1, 0.3*inch))

        elementos.append(Paragraph('ORDENES DE COMPRA GIRADAS', subtitulo_style))
        
        ocs = sorted(convenio.get('ocs', []), key=lambda x: x.get('fecha_emision') or '0000-00-00')
        total_girado = sum(float(oc.get('monto', 0)) for oc in ocs)
        monto_adjudicado = float(convenio.get('monto_adjudicado', 0) or 0)
        saldo = monto_adjudicado - total_girado

        style_header_table = ParagraphStyle(name='Header', fontName='Helvetica-Bold', fontSize=9, alignment=TA_LEFT)
        oc_data = [[Paragraph('N° ORDEN DE COMPRA', style_header_table), Paragraph('MONTO GIRADO', style_header_table)]]
        
        if ocs:
            for oc in ocs:
                oc_data.append([Paragraph(oc.get('numero_oc', 'N/A'), style_value), Paragraph(formatear_monto(oc.get('monto', 0)), style_value)])
        else:
            oc_data.append([Paragraph('Sin Órdenes de Compra', style_value), Paragraph('$0', style_value)])

        oc_data.append([Paragraph('TOTAL', style_label), Paragraph(formatear_monto(total_girado), style_label)])
        oc_data.append([Paragraph('SALDO', style_label), Paragraph(formatear_monto(saldo), style_label)])

        oc_table = Table(oc_data, colWidths=[3.5*inch, 3.5*inch])
        oc_table.setStyle(TableStyle([
            ('GRID', (0,0), (-1,-1), 1, colors.lightgrey),
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#EFEFEF')),
            ('ALIGN', (1,0), (1,-1), 'RIGHT'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('RIGHTPADDING', (0,0), (-1,-1), 8),
        ]))
        elementos.append(oc_table)

        if i < num_proveedores - 1:
            elementos.append(PageBreak())

    doc.build(elementos)
    return nombre_archivo