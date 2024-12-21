import json
import boto3
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass
from botocore.exceptions import ClientError
from botocore.signers import CloudFrontSigner

# Configure logging
logger = logging.getLogger('QueryProcessor')
logger.setLevel(logging.INFO)


def load_manifest() -> dict:
    """Load manifest.json from the current directory and return as dict."""
    try:
        with open('manifest.json', 'r') as f:
            manifest = json.loads(f.read())
        return manifest
    except FileNotFoundError:
        raise FileNotFoundError("manifest.json not found in current directory")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding manifest.json: {str(e)}")


class RequestIdFilter(logging.Filter):
    def __init__(self):
        self.request_id = None

    def filter(self, record):
        record.request_id = self.request_id or 'NO_REQUEST_ID'
        return True


formatter = logging.Formatter('%(asctime)s - %(request_id)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
request_id_filter = RequestIdFilter()
logger.addHandler(handler)
logger.addFilter(request_id_filter)
manifest = load_manifest()


@dataclass
class ChunkMetadata:
    path: str
    hour_key: str  # Format: YYYY-MM-DD-HH
    time_range: Dict[str, str]
    record_count: int
    size: int


@dataclass
class QueryPlan:
    chunks: List[ChunkMetadata]
    filters: List[Dict]
    projections: List[str]
    estimated_size: int
    partition_count: int


s3_client = boto3.client('s3')


class SQLParser:
    """Simple SQL parser for basic SELECT queries with WHERE clauses"""

    def __init__(self, query: str):
        self.query = query.strip()
        self.tokens = self._tokenize(query)
        logger.info(f"Tokenized query into {len(self.tokens)} tokens")

    def _tokenize(self, query: str) -> List[str]:
        """Split query into tokens, preserving quoted strings"""
        # Replace newlines and extra spaces
        query = ' '.join(query.split())

        # Split on spaces while preserving quoted strings
        tokens = []
        current_token = ''
        in_quotes = False
        quote_char = None

        for char in query:
            if char in ["'", '"']:
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                current_token += char
            elif char.isspace() and not in_quotes:
                if current_token:
                    tokens.append(current_token)
                    current_token = ''
            else:
                current_token += char

        if current_token:
            tokens.append(current_token)

        return tokens

    def extract_projections(self) -> List[str]:
        """Extract column names from SELECT clause"""
        try:
            select_idx = self.tokens.index('SELECT')
            from_idx = self.tokens.index('FROM')

            projection_str = ' '.join(self.tokens[select_idx + 1:from_idx])
            projections = [col.strip() for col in projection_str.split(',')]
            logger.info(f"Extracted projections: {projections}")
            return projections

        except ValueError:
            logger.error("Invalid SQL: Missing SELECT or FROM clause")
            raise ValueError("Invalid SQL: Missing SELECT or FROM clause")

    def extract_time_range(self) -> Optional[Dict[str, str]]:
        """Extract time range from BETWEEN clause"""
        try:
            where_idx = self.tokens.index('WHERE')
            where_clause = ' '.join(self.tokens[where_idx + 1:])

            # Find BETWEEN clause for timestamp
            between_match = re.search(
                r"time\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'",
                where_clause,
                re.IGNORECASE
            )

            if between_match:
                start_time, end_time = between_match.groups()
                logger.info(f"Extracted time range: {start_time} to {end_time}")
                return {'start': start_time, 'end': end_time}

            return None

        except ValueError:
            return None

    def extract_filters(self) -> List[Dict]:
        """Extract all WHERE conditions"""
        filters = []
        try:
            where_idx = self.tokens.index('WHERE')
            where_clause = ' '.join(self.tokens[where_idx + 1:])

            # Split on AND, ignoring ANDs in BETWEEN clauses
            conditions = []
            current_condition = ''
            in_between = False

            for token in where_clause.split():
                if token.upper() == 'BETWEEN':
                    in_between = True
                elif token.upper() == 'AND':
                    if in_between:
                        current_condition += ' ' + token
                        in_between = False
                    else:
                        if current_condition:
                            conditions.append(current_condition.strip())
                            current_condition = ''
                else:
                    current_condition += ' ' + token

            if current_condition:
                conditions.append(current_condition.strip())

            # Create filter objects
            for condition in conditions:
                if 'BETWEEN' not in condition.upper():
                    filters.append({'condition': condition.strip()})
                    logger.info(f"Added filter condition: {condition.strip()}")

            return filters

        except ValueError:
            return []


class QueryProcessor:
    def __init__(self):
        logger.info("Initializing QueryProcessor")
        self.s3 = boto3.client('s3')
        self.BUCKET = 'fresco-data-source'
        self.REGION = 'us-east-1'  # Add region
        self.MAX_CONCURRENT_CHUNKS = 4
        self.MAX_RESPONSE_TIME = 60
        self.CHUNK_SIZE_TARGET = 50 * 1024 * 1024
        logger.info(f"Configuration: BUCKET={self.BUCKET}, MAX_CONCURRENT_CHUNKS={self.MAX_CONCURRENT_CHUNKS}")

    def get_hour_key(self, timestamp: datetime) -> str:
        """Generate consistent hour-based key for chunks"""
        hour_key = timestamp.strftime('%Y-%m-%d-%H')
        logger.info(f"Generated hour key: {hour_key} for timestamp {timestamp}")
        return hour_key

    def get_chunk_path(self, hour_key: str) -> str:
        """Generate S3 path for a given hour chunk"""
        # Parse the hour_key (format: YYYY-MM-DD-HH)
        parts = hour_key.split('-')
        year, month, day, hour = parts

        # Construct path in the correct format: chunks/YYYY/MM/DD/HH.parquet
        path = f'chunks/{year}/{month}/{day}/{hour}.parquet'
        logger.info(f"Generated chunk path: {path}")
        return path

    def generate_public_urls(self, chunks: List[ChunkMetadata]) -> List[Dict]:
        """Generate public S3 URLs for chunk access"""
        logger.info(f"Generating public S3 URLs for {len(chunks)} chunks")
        urls = []

        try:
            for chunk in chunks:
                # Get a new presigned URL with the current credentials
                presigned_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': self.BUCKET,
                        'Key': chunk.path
                    },
                    ExpiresIn=3600,  # URL expires in 1 hour
                    HttpMethod='GET'
                )

                urls.append({
                    'url': presigned_url,
                    'hourKey': chunk.hour_key,
                    'timeRange': chunk.time_range,
                    'size': chunk.size
                })
                logger.info(f"Generated presigned URL for chunk {chunk.hour_key}")

            logger.info(f"Generated {len(urls)} URLs")
            return urls

        except Exception as e:
            logger.error(f"Error generating URLs: {str(e)}")
            raise

    def parse_sql(self, sql: str) -> Tuple[List[str], List[Dict], Dict]:
        """Parse SQL query to extract projections, filters, and time range"""
        logger.info(f"Parsing SQL query: {sql}")
        parser = SQLParser(sql)

        projections = parser.extract_projections()
        time_range = parser.extract_time_range()
        filters = parser.extract_filters()

        if not time_range:
            logger.error("Missing required time range in query")
            raise ValueError("Query must include a timestamp BETWEEN clause")

        logger.info(f"SQL parsing complete. Projections: {len(projections)}, "
                    f"Filters: {len(filters)}, Time range: {time_range}")
        return projections, filters, time_range

    def get_required_hours(self, start_time: datetime, end_time: datetime) -> Set[str]:
        """Get set of hour keys needed to cover the time range"""
        logger.info(f"Calculating required hours between {start_time} and {end_time}")
        required_hours = set()
        current = start_time.replace(minute=0, second=0, microsecond=0)

        while current <= end_time:
            required_hours.add(self.get_hour_key(current))
            current += timedelta(hours=1)

        logger.info(f"Found {len(required_hours)} required hour chunks")
        return required_hours

    def get_relevant_chunks(self, manifest: Dict, time_range: Dict) -> List[ChunkMetadata]:
        """Find chunks that intersect with the query time range"""
        logger.info(f"Finding relevant chunks for time range: {time_range}")
        start_time = datetime.fromisoformat(time_range['start'])
        end_time = datetime.fromisoformat(time_range['end'])

        required_hours = self.get_required_hours(start_time, end_time)

        relevant_chunks = []
        total_size = 0

        for hour_key in required_hours:
            chunk_info = manifest['chunks'].get(hour_key)
            if chunk_info:
                metadata = ChunkMetadata(
                    path=self.get_chunk_path(hour_key),
                    hour_key=hour_key,
                    time_range=chunk_info['timeRange'],
                    record_count=chunk_info['recordCount'],
                    size=chunk_info['sizeBytes']
                )
                relevant_chunks.append(metadata)
                total_size += chunk_info['sizeBytes']
                logger.info(f"Found chunk for hour {hour_key}: size={chunk_info['sizeBytes']}")

        if not relevant_chunks:
            logger.error(f"No data found for time range {time_range['start']} to {time_range['end']}")
            raise Exception(f"No data found for time range {time_range['start']} to {time_range['end']}")

        total_size_mb = total_size / 1024 / 1024
        if total_size > self.MAX_CONCURRENT_CHUNKS * self.CHUNK_SIZE_TARGET:
            logger.error(f"Query would return too much data ({total_size_mb:.2f}MB)")
            raise Exception(f"Query would return too much data ({total_size_mb:.2f}MB). Please narrow time range.")

        return relevant_chunks

    def optimize_chunk_distribution(self, chunks: List[ChunkMetadata]) -> int:
        """Determine optimal number of parallel downloads based on chunk sizes"""
        total_size = sum(chunk.size for chunk in chunks)
        logger.info(f"Optimizing chunk distribution for {len(chunks)} chunks, "
                    f"total size: {total_size / 1024 / 1024:.2f}MB")

        if total_size < self.CHUNK_SIZE_TARGET:
            logger.info("Total size below target, using single partition")
            return 1

        partition_count = min(
            self.MAX_CONCURRENT_CHUNKS,
            len(chunks),
            max(1, round(total_size / self.CHUNK_SIZE_TARGET))
        )

        logger.info(f"Determined optimal partition count: {partition_count}")
        return partition_count

    def create_query_plan(self, sql: str, manifest: Dict) -> QueryPlan:
        """Create execution plan based on SQL query and manifest"""
        logger.info("Creating query plan")
        projections, filters, time_range = self.parse_sql(sql)

        chunks = self.get_relevant_chunks(manifest, time_range)
        chunks.sort(key=lambda x: x.hour_key)

        partition_count = self.optimize_chunk_distribution(chunks)
        estimated_size = sum(chunk.size for chunk in chunks)

        plan = QueryPlan(
            chunks=chunks,
            filters=filters,
            projections=projections,
            estimated_size=estimated_size,
            partition_count=partition_count
        )

        logger.info(f"Query plan created: {len(chunks)} chunks, {partition_count} partitions")
        return plan

    def generate_presigned_urls(self, chunks: List[ChunkMetadata]) -> List[Dict]:
        """Generate signed URLs for chunk access through CloudFront"""
        logger.info(f"Generating CloudFront signed URLs for {len(chunks)} chunks")
        urls = []

        # Create timezone-aware datetime
        from zoneinfo import ZoneInfo
        expiration = datetime.now(ZoneInfo("UTC")) + timedelta(minutes=15)

        # Get private key from Secrets Manager
        try:
            secret_name = "CLOUDFRONT_PRIVATE_KEY"
            region_name = "us-east-1"

            session = boto3.session.Session()
            secrets_client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )

            logger.info("Retrieving CloudFront private key from Secrets Manager")
            secret_response = secrets_client.get_secret_value(
                SecretId=secret_name
            )
            private_key = secret_response['SecretString']
            logger.info("Successfully retrieved private key from Secrets Manager")

        except ClientError as e:
            logger.error(f"Error retrieving secret: {str(e)}")
            raise

        try:
            # Create CloudFront signer using the imported CloudFrontSigner class
            signer = CloudFrontSigner(
                self.CLOUDFRONT_KEY_ID,
                lambda message: private_key.encode('ascii')
            )
            logger.info("Created CloudFront signer")

            for chunk in chunks:
                # Remove any leading https:// from domain if present
                domain = self.CLOUDFRONT_DOMAIN.replace('https://', '')

                # Construct the CloudFront URL
                cloudfront_url = f'https://{domain}/{chunk.path}'

                # Generate signed URL
                signed_url = signer.generate_presigned_url(
                    cloudfront_url,
                    date_less_than=expiration
                )

                urls.append({
                    'url': signed_url,
                    'hourKey': chunk.hour_key,
                    'timeRange': chunk.time_range,
                    'size': chunk.size
                })
                logger.info(f"Generated signed CloudFront URL for chunk {chunk.hour_key}")

            logger.info(f"Generated {len(urls)} signed URLs, expiring at {expiration}")
            return urls

        except Exception as e:
            logger.error(f"Error generating signed URLs: {str(e)}")
            raise

    # def generate_public_urls(self, chunks: List[ChunkMetadata]) -> List[Dict]:
    #     """Generate public URLs for chunk access through CloudFront"""
    #     logger.info(f"Generating public CloudFront URLs for {len(chunks)} chunks")
    #     urls = []
    #
    #     try:
    #         for chunk in chunks:
    #             # Remove any leading https:// from domain if present
    #             domain = self.CLOUDFRONT_DOMAIN.replace('https://', '')
    #
    #             # Construct the public CloudFront URL
    #             public_url = f'https://{domain}/{chunk.path}'
    #
    #             urls.append({
    #                 'url': public_url,
    #                 'hourKey': chunk.hour_key,
    #                 'timeRange': chunk.time_range,
    #                 'size': chunk.size
    #             })
    #             logger.info(f"Generated public CloudFront URL for chunk {chunk.hour_key}")
    #
    #         logger.info(f"Generated {len(urls)} public URLs")
    #         return urls
    #
    #     except Exception as e:
    #         logger.error(f"Error generating public URLs: {str(e)}")
    #         raise


def lambda_handler(event, context):
    logger.info(F"Incoming event: {event}")

    try:
        sql_query = event['query']
        logger.info(f"Received SQL query: {sql_query}")

        processor = QueryProcessor()

        logger.info(f"Fetching manifest from {processor.BUCKET}/manifest.json")

        plan = processor.create_query_plan(sql_query, manifest)
        # Use the new generate_public_urls method instead of generate_presigned_urls
        chunk_urls = processor.generate_public_urls(plan.chunks)

        response = {
            'statusCode': 200,
            'body': json.dumps({
                'transferId': context.aws_request_id,
                'metadata': {
                    'total_partitions': plan.partition_count,
                    'estimated_size': plan.estimated_size,
                    'chunk_count': len(chunk_urls),
                    'hour_count': len(set(chunk.hour_key for chunk in plan.chunks))
                },
                'chunks': chunk_urls,
                'queryPlan': {
                    'projections': plan.projections,
                    'filters': plan.filters
                }
            })
        }

        logger.info(f"Request {context.aws_request_id} completed successfully")
        return response

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
