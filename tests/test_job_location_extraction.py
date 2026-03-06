from __future__ import annotations

from app.tools import job_discovery as jd


def test_extract_location_from_jsonld_jobposting() -> None:
    html = """
    <html><head>
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "JobPosting",
        "title": "Software Engineer",
        "jobLocation": {
          "@type": "Place",
          "address": {
            "@type": "PostalAddress",
            "addressLocality": "Warsaw",
            "addressCountry": "Poland"
          }
        }
      }
      </script>
    </head><body><p>Apply now</p></body></html>
    """
    text = jd._extract_posting_text(html)
    location = jd._extract_location_from_posting(html, text, "United States")
    assert location == "Warsaw, Poland"


def test_extract_location_from_structured_inline_json() -> None:
    html = """
    <script>
      window.__DATA__ = {
        "standardised_multi_location": [
          {
            "standardisedMapQueryLocation": "Fort Worth, Texas, United States"
          }
        ]
      };
    </script>
    <div>Engineering role</div>
    """
    text = jd._extract_posting_text(html)
    location = jd._extract_location_from_posting(html, text, "United States")
    assert location == "Fort Worth, Texas, United States"


def test_location_defaults_only_when_not_found() -> None:
    html = "<html><body><p>No location field in this content.</p></body></html>"
    text = jd._extract_posting_text(html)
    location = jd._extract_location_from_posting(html, text, "United States")
    assert location == "United States"
