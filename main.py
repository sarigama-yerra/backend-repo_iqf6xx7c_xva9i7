import io
import os
from typing import List, Optional

from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI(title="PDFMaster Pro API", version="1.0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def stream_bytes(data: bytes, filename: str, media_type: str = "application/octet-stream"):
    return StreamingResponse(io.BytesIO(data), media_type=media_type, headers={
        "Content-Disposition": f"attachment; filename={filename}",
        "Cache-Control": "no-store",
    })


@app.get("/")
async def root():
    return {"name": "PDFMaster Pro API", "status": "ok"}


# Lazy import helpers to avoid startup failures if optional libs are missing

def _require_pypdf2():
    try:
        from PyPDF2 import PdfReader, PdfWriter  # type: ignore
        return PdfReader, PdfWriter
    except ModuleNotFoundError as e:
        raise HTTPException(500, detail=f"Server missing dependency PyPDF2: {e}")


def _require_pikepdf():
    try:
        import pikepdf  # type: ignore
        return pikepdf
    except ModuleNotFoundError as e:
        raise HTTPException(500, detail=f"Server missing dependency pikepdf: {e}")


def _require_pillow():
    try:
        from PIL import Image  # type: ignore
        return Image
    except ModuleNotFoundError as e:
        raise HTTPException(500, detail=f"Server missing dependency Pillow: {e}")


def _require_pdfium():
    try:
        import pypdfium2 as pdfium  # type: ignore
        return pdfium
    except ModuleNotFoundError as e:
        raise HTTPException(500, detail=f"Server missing dependency pypdfium2: {e}")


@app.post("/api/merge")
async def merge_pdfs(files: List[UploadFile] = File(...)):
    PdfReader, PdfWriter = _require_pypdf2()
    if not files or len(files) < 2:
        raise HTTPException(status_code=400, detail="Please upload at least two PDF files to merge.")
    writer = PdfWriter()
    try:
        for f in files:
            content = await f.read()
            try:
                reader = PdfReader(io.BytesIO(content))
            except Exception as e:
                raise HTTPException(400, detail=f"Failed to read {f.filename}: {e}")
            for page in reader.pages:
                writer.add_page(page)
        out = io.BytesIO()
        writer.write(out)
        out.seek(0)
        return stream_bytes(out.read(), "merged.pdf", media_type="application/pdf")
    finally:
        for f in files:
            await f.close()


@app.post("/api/split")
async def split_pdf(file: UploadFile = File(...), ranges: Optional[str] = Form(None)):
    PdfReader, PdfWriter = _require_pypdf2()
    content = await file.read()
    try:
        reader = PdfReader(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, detail=f"Failed to read PDF: {e}")

    total = len(reader.pages)

    def parse_ranges(r: Optional[str]):
        if not r:
            return [[1, total]]
        parts = []
        for seg in r.split(','):
            seg = seg.strip()
            if '-' in seg:
                a, b = seg.split('-')
                parts.append([max(1, int(a)), min(total, int(b))])
            else:
                v = int(seg)
                parts.append([max(1, v), min(total, v)])
        res = []
        for a, b in parts:
            if a > b:
                a, b = b, a
            res.append([a, b])
        return res

    chunks = parse_ranges(ranges)
    outputs = []

    for idx, (a, b) in enumerate(chunks, start=1):
        writer = PdfWriter()
        for i in range(a - 1, b):
            writer.add_page(reader.pages[i])
        buf = io.BytesIO()
        writer.write(buf)
        outputs.append((f"split_{idx}.pdf", buf.getvalue()))

    if len(outputs) == 1:
        name, data = outputs[0]
        return stream_bytes(data, name, "application/pdf")
    else:
        import zipfile
        mem = io.BytesIO()
        with zipfile.ZipFile(mem, 'w', zipfile.ZIP_DEFLATED) as z:
            for name, data in outputs:
                z.writestr(name, data)
        mem.seek(0)
        return stream_bytes(mem.read(), "splits.zip", "application/zip")


@app.post("/api/compress")
async def compress_pdf(file: UploadFile = File(...), level: str = Form("medium")):
    pikepdf = _require_pikepdf()
    level = level.lower()
    if level not in {"low", "medium", "high"}:
        raise HTTPException(400, detail="level must be one of: low, medium, high")

    settings = {
        "low": dict(image_quality=0.6),
        "medium": dict(image_quality=0.4),
        "high": dict(image_quality=0.25),
    }[level]

    data = await file.read()
    try:
        with pikepdf.open(io.BytesIO(data)) as pdf:
            from pikepdf import PdfImage
            for page in pdf.pages:
                xobjects = page.resources.get('/XObject', {}) if hasattr(page, 'resources') else {}
                for _, raw in list(getattr(xobjects, 'items', lambda: [])()):
                    try:
                        xobj = raw.get_object()
                        if xobj.get('/Subtype') == '/Image':
                            img = PdfImage(xobj)
                            pil = img.as_pil_image()
                            buf = io.BytesIO()
                            pil.save(buf, format='JPEG', quality=int(settings['image_quality']*100))
                            buf.seek(0)
                            img.replace(pikepdf.Stream(pdf, buf.read()), format='JPEG')
                    except Exception:
                        continue
            out = io.BytesIO()
            pdf.save(out, compress_streams=True, object_stream_mode=pikepdf.ObjectStreamMode.generate, qdf=False)
            out.seek(0)
            return stream_bytes(out.read(), f"compressed_{level}.pdf", "application/pdf")
    except Exception as e:
        raise HTTPException(400, detail=f"Compression failed: {e}")


@app.post("/api/image-to-pdf")
async def image_to_pdf(files: List[UploadFile] = File(...)):
    Image = _require_pillow()
    if not files:
        raise HTTPException(400, detail="Upload at least one image")
    images: List["Image.Image"] = []
    try:
        for f in files:
            raw = await f.read()
            img = Image.open(io.BytesIO(raw)).convert('RGB')
            images.append(img)
        buf = io.BytesIO()
        first, rest = images[0], images[1:]
        first.save(buf, format='PDF', resolution=300.0, save_all=True, append_images=rest)
        return stream_bytes(buf.getvalue(), "images.pdf", "application/pdf")
    except Exception as e:
        raise HTTPException(400, detail=f"Image to PDF failed: {e}")
    finally:
        for img in images:
            try:
                img.close()
            except Exception:
                pass


@app.post("/api/pdf-to-image")
async def pdf_to_image(file: UploadFile = File(...)):
    pdfium = _require_pdfium()
    data = await file.read()
    try:
        pdf = pdfium.PdfDocument(io.BytesIO(data))
        pngs: List[bytes] = []
        for i in range(len(pdf)):
            page = pdf[i]
            pil_image = page.render(scale=2).to_pil()
            b = io.BytesIO()
            pil_image.save(b, format='PNG')
            pngs.append(b.getvalue())
        if len(pngs) == 1:
            return stream_bytes(pngs[0], "page-1.png", "image/png")
        else:
            import zipfile
            mem = io.BytesIO()
            with zipfile.ZipFile(mem, 'w', zipfile.ZIP_DEFLATED) as z:
                for idx, data in enumerate(pngs, start=1):
                    z.writestr(f"page-{idx}.png", data)
            return stream_bytes(mem.getvalue(), "pages.zip", "application/zip")
    except Exception as e:
        raise HTTPException(400, detail=f"PDF to Image failed: {e}")


@app.post("/api/unlock")
async def unlock_pdf(file: UploadFile = File(...), password: Optional[str] = Form(None)):
    PdfReader, PdfWriter = _require_pypdf2()
    import pikepdf as _pp  # optional but lightweight import here
    data = await file.read()
    try:
        reader = PdfReader(io.BytesIO(data))
        if getattr(reader, 'is_encrypted', False):
            if password:
                ok = reader.decrypt(password)
                if ok == 0:
                    raise HTTPException(400, detail="Incorrect password")
            else:
                try:
                    with _pp.open(io.BytesIO(data)) as pdf:
                        out = io.BytesIO()
                        pdf.save(out)
                        return stream_bytes(out.getvalue(), "unlocked.pdf", "application/pdf")
                except Exception:
                    raise HTTPException(400, detail="Password required to unlock")
        writer = PdfWriter()
        for p in reader.pages:
            writer.add_page(p)
        out = io.BytesIO()
        writer.write(out)
        return stream_bytes(out.getvalue(), "unlocked.pdf", "application/pdf")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, detail=f"Unlock failed: {e}")


@app.post("/api/watermark")
async def watermark_pdf(
    file: UploadFile = File(...),
    text: str = Form("CONFIDENTIAL"),
    position: str = Form("center"),
):
    PdfReader, PdfWriter = _require_pypdf2()
    Image = _require_pillow()
    data = await file.read()
    try:
        reader = PdfReader(io.BytesIO(data))
        writer = PdfWriter()
        for page in reader.pages:
            mediabox = page.mediabox
            width = int(float(mediabox.width))
            height = int(float(mediabox.height))
            img = Image.new('RGBA', (max(1, width), max(1, height)), (0, 0, 0, 0))
            from PIL import ImageDraw, ImageFont
            draw = ImageDraw.Draw(img)
            try:
                font = ImageFont.truetype("DejaVuSans.ttf", size=max(14, min(width, height)//18))
            except Exception:
                font = ImageFont.load_default()
            tw, th = draw.textsize(text, font=font)
            pos_map = {
                "top-left": (20, 20),
                "top-right": (img.width - tw - 20, 20),
                "center": ((img.width - tw)//2, (img.height - th)//2),
            }
            xy = pos_map.get(position, pos_map["center"])
            draw.text(xy, text, fill=(0, 0, 0, 100), font=font)
            overlay_buf = io.BytesIO()
            img_rgb = img.convert('RGB')
            img_rgb.save(overlay_buf, format='PDF')
            overlay_pdf = PdfReader(io.BytesIO(overlay_buf.getvalue()))
            overlay_page = overlay_pdf.pages[0]
            page.merge_page(overlay_page)
            writer.add_page(page)
        out = io.BytesIO()
        writer.write(out)
        return stream_bytes(out.getvalue(), "watermarked.pdf", "application/pdf")
    except Exception as e:
        raise HTTPException(400, detail=f"Watermark failed: {e}")


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": True, "detail": exc.detail})


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
