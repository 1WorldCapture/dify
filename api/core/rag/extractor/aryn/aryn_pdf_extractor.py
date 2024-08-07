import logging
from typing import Optional

from aryn_sdk.partition import partition_file, table_elem_to_dataframe

from core.rag.extractor.extractor_base import BaseExtractor
from core.rag.models.document import Document

logger = logging.getLogger(__name__)

class ArynPDFExtractor(BaseExtractor):

    def __init__(self,
                 file_path: str,
                 api_key: str):
        self._file_path = file_path
        self._api_key = api_key

    def extract(self) -> list[Document]:
        logger.info(f"Starting Aryn PDF extraction for file: {self._file_path}")
        documents = []

        def partition_filepath(filelocation, api_key=None, **options):
            with open(filelocation, "rb") as f:
                return partition_file(f, api_key, **options)

        response_json = partition_filepath(
            self._file_path,
            api_key=self._api_key,
            threshold=0.3,
            extract_table_structure=True,
            extract_images=True,
            use_ocr=True,
        )

        tups = self.elements_to_tups(response_json.get('elements', []))

        for header, value in tups:
            header = header.strip() if header else None
            value = value.strip()
            if header is None:
                documents.append(Document(page_content=value))
            else:
                documents.append(Document(page_content=f"\n\n{header}\n{value}"))

        return documents

    def elements_to_tups(self, elements: list[dict]) -> list[tuple[Optional[str], str]]:
        tups: list[tuple(Optional[str], str)] = []

        current_header = None
        current_text = ""

        for element in elements:
            if element['type'] == 'Text':
                current_text += element['text_representation']
            elif element['type'] == 'List-item':
                current_text += element['text_representation']
            elif element['type'] == 'table':
                df = table_elem_to_dataframe(element)
                current_text += df.to_string()

            if element['type'] == 'Section-header' or element['type'] == 'Title':
                if current_text != "":
                    tups.append((current_header, current_text))

                current_header = element['text_representation']
                current_text = ""

        if current_text != "":
            tups.append((current_header, current_text))

        return tups