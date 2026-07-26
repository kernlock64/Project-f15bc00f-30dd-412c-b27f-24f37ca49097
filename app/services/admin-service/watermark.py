from PIL import Image
import io


def watermark(stream):
    base_img = Image.open(stream).convert("RGBA")
    logo = Image.open("static/watermark.jpeg").convert("RGBA")
    r, g, b, alpha = logo.split()
    logo = Image.merge("RGB", (r, g, b)).convert("L").convert("RGBA")
    alpha = alpha.point(lambda p: int(p * (100/ 255.0)))
    logo.putalpha(alpha)
    x = (base_img.width - logo.width) // 2
    y = (base_img.height - logo.height) // 2

    overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))

    overlay.paste(logo, (x, y), mask=logo)
    watermarked_img = Image.alpha_composite(base_img, overlay)
    final_img = watermarked_img.convert("RGB")

    output = io.BytesIO()
    final_img.save(output, format="JPEG")
    output.seek(0)

    return output