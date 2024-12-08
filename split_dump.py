import os
import bz2

# Define the maximum size per chunk (150 MB in bytes)
MAX_CHUNK_SIZE = 150 * 1024 * 1024  # 150 MB

def split_xml(filename):
    """Splits a large XML .bz2 file into smaller .bz2 files, ensuring each chunk is around 150MB in size."""
    
    if not os.path.exists("chunks"):
        os.mkdir("chunks")
    
    # Counters
    pagecount = 0
    filecount = 1
    current_chunk_size = 0  # To track the current chunk size
    
    chunkname = lambda filecount: os.path.join("chunks", f"chunk-{filecount}.xml.bz2")
    
    chunkfile = bz2.BZ2File(chunkname(filecount), 'w')
    
    with bz2.BZ2File(filename, 'rb') as bzfile:
        page_data = []  
        for line in bzfile:
            line_str = line.decode('utf-8')  
            page_data.append(line_str)
            
            if '</page>' in line_str:
                pagecount += 1
                
                page_data_bytes = ''.join(page_data).encode('utf-8')  # Re-encode to bytes for writing
                chunkfile.write(page_data_bytes)
                
                current_chunk_size += len(page_data_bytes)
                
                page_data = []
                
                if current_chunk_size >= MAX_CHUNK_SIZE:
                    chunkfile.close() 
                    pagecount = 0  
                    current_chunk_size = 0  
                    filecount += 1  
                    chunkfile = bz2.BZ2File(chunkname(filecount), 'w')  # Open a new chunk file
    
    try:
        chunkfile.close()
        print(f"Splitting complete. {filecount} chunk(s) created.")
    except Exception as e:
        print(f"Error closing the last chunk file: {e}")

if __name__ == '__main__':
    split_xml('dumps/simplewiki-20240901-pages-articles-multistream.xml.bz2')

