import json
import re
import logging
from typing import List, Dict, Optional
from faker import Faker
import random
from pathlib import Path

# Configure standard logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class LogGenerator:
    """Handles the generation of multi-lingual synthetic factory maintenance logs."""

    def __init__(self):
        # Initialize Faker with English, German, and Arabic locales
        self.fake = Faker(['en_US', 'de_DE', 'ar_EG'])

        # Scenarios mapped to specific operational contexts and languages
        self.templates = [
            {"lang": "en", "text": "Robotic arm calibration error on the digital production line. Assigned to {name}. Contact: {email} or {phone}."},
            {"lang": "de", "text": "Gabelstapler-Batterieproblem im Depot-Lagerraum in Pfarrkirchen. Gemeldet von {name}. Erreichbar unter {phone}, Email: {email}."},
            {"lang": "ar", "text": "عطل في المستشعر في منشأة تشام بالقرب من Ludwigstraße 6. الفني المسؤول: {name}. للتواصل: {phone} أو عبر البريد {email}."},
            {"lang": "en", "text": "Conveyor belt motor overheating in Sector 4. Maintenance requested by {name}. Notify via {email} or call {phone}."},
            {"lang": "de", "text": "Kühlmittelleck an CNC-Maschine 04. Techniker {name} ist vor Ort. Tel: {phone}. Bericht an {email} senden."},
            {"lang": "ar", "text": "توقف مفاجئ في خط التجميع الآلي. تم إبلاغ {name}. يرجى الاتصال على {phone} أو {email}."},
            {"lang": "en", "text": "Hydraulic pressure drop in the stamping press. {name} investigating. Direct line: {phone}. Logs sent to {email}."},
            {"lang": "de", "text": "Fehlfunktion des optischen Sensors an der Verpackungsstation. Bitte {name} unter {phone} oder {email} kontaktieren."},
            {"lang": "ar", "text": "ارتفاع درجة حرارة المولد الاحتياطي. المهندس {name} يتابع المشكلة. رقم الهاتف {phone}، البريد {email}."},
            {"lang": "en",
                "text": "Firmware update failed on PLC unit A1. {name} is handling the rollback. Reach out at {email} (Phone: {phone})."}
        ]

    def generate_logs(self, count: int = 20) -> List[Dict[str, str]]:
        logs = []
        for i in range(count):
            template = random.choice(self.templates)
            lang = template["lang"]

            # Generate locale-specific PII
            if lang == "en":
                name = self.fake['en_US'].name()
                phone = self.fake['en_US'].phone_number()
            elif lang == "de":
                name = self.fake['de_DE'].name()
                # Enforce standard German +49 formatting for validation
                phone = f"+49 1{random.randint(5,7)} {random.randint(1000000, 9999999)}"
            else:
                name = self.fake['ar_EG'].name()
                phone = self.fake['ar_EG'].phone_number()

            email = self.fake.ascii_safe_email()

            # Inject PII into the localized template
            raw_text = template["text"].format(
                name=name, email=email, phone=phone)

            logs.append({
                "ticket_id": f"TKT-{1000 + i}",
                "language": lang,
                "raw_log": raw_text,
                "_injected_name": name  # Tracked strictly to simulate MVP Entity Recognition
            })
        return logs


class PIIMasker:
    """Handles the redaction of sensitive information from text."""

    @staticmethod
    def mask(text: str, known_name: Optional[str] = None) -> str:
        """
        Redacts Emails, Phones, and Names.
        Uses Regex for pattern-based PII (Emails/Phones).
        Uses exact replacement for Names to emulate an NLP model output without heavy dependencies.
        """
        # 1. Mask Email (Standard robust regex)
        email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
        masked_text = re.sub(email_pattern, '[REDACTED_EMAIL]', text)

        # 2. Mask Phone (Handles broad international variants, spaces, dashes, and parentheses)
        phone_pattern = r'(\+?\d{1,4}[\s\-\.]?)?\(?\d{2,4}\)?[\s\-\.]?\d{3,4}[\s\-\.]?\d{3,4}'
        masked_text = re.sub(phone_pattern, '[REDACTED_PHONE]', masked_text)

        # 3. Mask Name
        # In a fully deployed pipeline, you would swap this logic out for Microsoft Presidio.
        if known_name:
            masked_text = masked_text.replace(known_name, '[REDACTED_NAME]')

        return masked_text


def main():
    generator = LogGenerator()
    masker = PIIMasker()

    # AC1 & AC2: Generate 20 multi-lingual scenarios
    raw_logs = generator.generate_logs(20)
    final_output = []

    # AC4: Console layout for verification
    print(
        f"| {'TICKET':<10} | {'RAW LOG (Truncated for display)':<55} | {'MASKED LOG'}")
    print("-" * 150)

    for log in raw_logs:
        # AC3: Apply masking
        masked_text = masker.mask(log["raw_log"], log["_injected_name"])

        # Print side-by-side verification
        print(
            f"| {log['ticket_id']:<10} | {log['raw_log'][:52] + '...':<55} | {masked_text}")

        final_output.append({
            "ticket_id": log["ticket_id"],
            "language": log["language"],
            "raw_log": log["raw_log"],
            "masked_log": masked_text
        })

    output_dir = Path("Data/Raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_filename = output_dir / "gfi_maintenance_logs.json"

    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(final_output, f, ensure_ascii=False, indent=4)

    print("-" * 150)
    logging.info(
        f"Successfully processed {len(final_output)} logs and saved to {output_filename}")


if __name__ == "__main__":
    main()
