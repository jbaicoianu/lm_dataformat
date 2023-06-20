import boto3
import botocore
import io
import threading
import multiprocessing
from collections import OrderedDict

# Represents an IO stream interface for reading and writing files directly to and from S3
# Intended to be passed in eg, as the fileobj parameter for a tarfile, to stream tar, zips, etc. remotely

# This was partly written with GPT4's help, but GPT4 is terrible at writing threaded code, so I had to heavily modify it anyway

class S3File(io.IOBase):
    def __init__(self, path, chunk_size=50 * 1024 * 1024):
        super().__init__()
        self.path = path
        self.chunk_size = chunk_size
        self.bucket, self.key = self._parse_s3_path(path)
        self.s3 = boto3.client('s3')
        self.file_size = self._get_file_size()
        self.cache = OrderedDict()
        self.offset = 0
        self.current_chunk = 0
        self.next_chunk = 0
        self.chunk_fetched = threading.Event()
        self.write_buffer = io.BytesIO()
        self.write_parts = []
        self.upload_id = None

        self.worker = None
        self.threadpool = None
        #print('new s3 file', self.path)


    def _parse_s3_path(self, path):
        assert path.startswith('s3://')
        path = path[len('s3://'):]
        parts = path.split('/', 1)
        return parts[0], parts[1]

    def create_download_worker(self):
        self.worker = threading.Thread(target=self._worker)
        self.worker.start()

    def create_upload_worker(self):
        self.threadpool = multiprocessing.pool.ThreadPool(2)

    def read(self, n=-1):
        if not self.worker:
            self.create_download_worker()
        #print('read called', n, self.tell())
        if n == -1:
            n = self.file_size - self.tell()
        result = bytearray()
        while n > 0:
            chunk = self.tell() // self.chunk_size
            offset = self.tell() % self.chunk_size

            data = self.cache.get(chunk)
            if not data or chunk != self.current_chunk:
                while data is None:
                    print('waiting on thread for chunk %d....' % chunk)
                    self.chunk_fetched.wait(.1)
                    data = self.cache.get(chunk)
                    if not data:
                        # Data underrun - our chunk size is probably too big, so we're waiting for chunks to download until we get back to processing
                        print('fetched null chunk, retry', chunk)
                        pass
            read_size = min(n, len(data) - offset)
            result.extend(data[offset:offset+read_size])
            self.seek(self.tell() + read_size)
            n -= read_size
            if read_size == 0 and n > 0:
                # no data left to read
                break
        return bytes(result)

    def _worker(self):
        # NOTE - as implemented, this chunked fetching logic will only work well for sequential reads
        # Random access, eg for reading zip files, will likely need some additional work, or smarter prefetching logic
        num_chunks = (self.file_size // self.chunk_size) + 1
        while self.next_chunk < num_chunks:
            current_chunk = self.tell() // self.chunk_size
            chunk = self.next_chunk
            if chunk not in self.cache:
                #self.chunk_fetched.clear()
                data = self._get_chunk(chunk)
                self.cache[chunk] = data
                self.chunk_fetched.set()
                self.next_chunk += 1
                #while len(self.cache) > 5:
                #    self.cache.popitem(last=False)
                items = self.cache.items()
                deletes = []
                for (f, data) in items:
                    if f < current_chunk:
                        deletes.append(f)

                for f in deletes:
                    del self.cache[f]
        self.chunk_fetched.clear()

    def _get_chunk(self, chunk):
        start = chunk * self.chunk_size
        end = min((chunk + 1) * self.chunk_size - 1, self.file_size)
        #print(f'fetch chunk {chunk} range {start}-{end}', flush=True)
        response = self.s3.get_object(Bucket=self.bucket, Key=self.key, Range=f'bytes={start}-{end}')
        body = response['Body'].read()
        length = len(body)
        #print(f'got chunk {chunk} range {start}-{end} ({length} bytes)')
        return body

    def seek(self, offset):
        self.offset = offset
        self.next_chunk = offset // self.chunk_size

    def tell(self):
        return self.offset

    def _get_file_size(self):
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=self.key)
            return response['ContentLength']
        except botocore.exceptions.ClientError:
            return 0


    def _upload_part(self, part_number, data):
        if not self.upload_id:
            self.upload_id = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.key)['UploadId']
            print('new upload id: ' + self.bucket, self.key, self.upload_id)

        print('upload part', part_number, len(data))
        response = self.s3.upload_part(
            Body=data,
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            PartNumber=part_number,
        )
        print(response)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.write_parts.append({'ETag': response['ETag'], 'PartNumber': part_number})

    def write(self, b):
        if self.closed:
            raise ValueError("I/O operation on closed file")

        if not self.threadpool:
            self.create_upload_worker()

        length = self.write_buffer.write(b)
        self.offset += length

        if self.write_buffer.tell() >= self.chunk_size:
            part_number = self.offset // self.chunk_size
            data = self.write_buffer.getvalue()
            self.threadpool.apply_async(func=self._upload_part, args=(part_number, data))

            # Clear the buffer
            self.write_buffer = io.BytesIO()
        return length

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.offset = offset
        elif whence == io.SEEK_CUR:
            self.offset += offset
        elif whence == io.SEEK_END:
            raise IOError("seek from the end not supported")
        else:
            raise ValueError("Invalid value for whence")

        return self.offset
    def flush(self):
        if self.write_buffer.tell() > 0:
            #part_number = len(self.write_parts) + 1
            part_number = self.offset // self.chunk_size + 1
            data = self.write_buffer.getvalue()
            self.threadpool.apply_async(func=self._upload_part, args=(part_number, data))

            # Clear the buffer
            self.write_buffer = io.BytesIO()

    def close(self):
        if self.closed:
            print('already closed')
            return
        self.flush()
        print('close it!', flush=True)
        if self.threadpool:
            self.threadpool.close()
            self.threadpool.join()
            #if self.buffer.tell() > 0:
            #    self.flush()
            print('upload finished', self.upload_id, self.bucket, self.key, self.write_parts)
            try:
                parts = sorted(self.write_parts, key=lambda part: part['PartNumber'])
                response = self.s3.complete_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.key,
                    UploadId=self.upload_id,
                    MultipartUpload={'Parts': parts},
                )
                print('Multipart upload completed')
            except Exception as e:
                print('Multipart upload failed: ', str(e), self.bucket)
                print(parts)
            #self.closed = True
        super().close()

os_open = open

def open(path, *args, **kwargs):
    if path[0:5] == 's3://':
        return S3File(path, **kwargs)
    else:
        return os_open(path, *args, **kwargs)
