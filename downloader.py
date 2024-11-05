import sys
import os
import subprocess
from multiprocessing.pool import ThreadPool
from yt_dlp import YoutubeDL
from pathlib import Path
import time
from tqdm import tqdm
import json
class VidInfo:
    def __init__(self, yt_id, start_time, end_time, outdir, download_audio=False, video_format='best',audioOutputDir=None):
        self.yt_id = yt_id
        self.start_time = float(start_time)
        self.end_time = float(end_time)
        self.outdir = os.path.join(outdir, str(yt_id))
        self.video_out_filename = os.path.join(self.outdir, f"{yt_id}_{start_time}_{end_time}_video.mp4")
        self.audio_out_filename = os.path.join(audioOutputDir, f"{yt_id}_{start_time}_{end_time}_audio.mp3")
        self.error_out_filename = os.path.join(self.outdir, f"{yt_id}_{start_time}_{end_time}_error.txt")
        self.done_out_filename = os.path.join(self.outdir, f"{yt_id}_{start_time}_{end_time}_done.txt")
        self.download_audio = download_audio
        self.video_format = video_format
        
    

    def create_outdir(self):
        os.makedirs(self.outdir, exist_ok=True)
class MyLogger:
    def __init__(self, log_file):
        self.log_file = log_file
    
    def error(self, msg):
        with open(self.log_file, 'a') as f:
            f.write("ERROR: " + msg + "\n")
    
    def debug(self, msg):
        # Optionally capture debug messages
        pass
    
    def warning(self, msg):
        # Optionally capture warnings
        pass
def getURL(yt_url, log_file='yt.log'):
    logger = mylogger = MyLogger(log_file)
    ydl_opts = {
        'listformats': True,
        'quiet': True,
        'ignoreerrors': True,
        'no_warnings': True,
        'logger': logger,
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url=yt_url, download=False)
        formats = info.get('formats', [])
        urls = {}
        for f in formats:            
            if "width" in f and f["protocol"] in ["https","dash","m3u8_native"] and f["width"] is not None:
                if f["width"] not in urls:
                    urls[f["width"]] = f
                else:
                    if f["tbr"] > urls[f["width"]]["tbr"]:
                        urls[f["width"]] = f
        #get biggest width
        max_width = max(urls.keys())
        return urls[max_width]



def download(vidinfo):
    vidinfo.create_outdir()
    yt_base_url = 'https://www.youtube.com/watch?v='
    yt_url = yt_base_url + vidinfo.yt_id
    # if not Path(vidinfo.audio_out_filename).exists():
    #     print(vidinfo.audio_out_filename)
    #     return "Not english"

    ydl_opts = {
        'format': vidinfo.video_format,
        'quiet': True,
        'ignoreerrors': True,
        'no_warnings': True,
    }

    if vidinfo.download_audio:
        ydl_opts['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }] #TODO: this will do nothing

    try:
        f = getURL(yt_url, log_file=vidinfo.error_out_filename)
    except Exception as e:
        # read error file					
        if Path(vidinfo.error_out_filename).exists():
            with open(vidinfo.error_out_filename, 'r') as f:
                error_msg = f.read()
            if 'not a bot.' in error_msg:
                return_msg = f'{vidinfo.yt_id}, ERROR (bot)!'
            #delete error file
                os.remove(vidinfo.error_out_filename)
                time.sleep(60)
                return return_msg,vidinfo
        return_msg = f'{vidinfo.yt_id}, ERROR (youtube)!'
        #touch error file
        open(vidinfo.error_out_filename, 'w').close()
        return return_msg,vidinfo
    video_url = f['url']
    bitrate = f['tbr']
    width = f['width']
    originalFile = vidinfo.video_out_filename.replace("dataTrainNew","dataTrain")
    if Path(originalFile).exists():
        oGBitRate = get_total_bitrate_with_ffprobe(originalFile)
        ogwidth = get_width_with_ffprobe(originalFile)
        if oGBitRate is not None:
            ratio = (bitrate-oGBitRate)/bitrate
            if ratio < 0.1 and ogwidth == width:
                #touch done file
                Path(vidinfo.done_out_filename).touch()
                return f'{vidinfo.yt_id}, DONE!',vidinfo
            else:
                #unlink original file
                Path(originalFile).unlink()
            

   
    #remove error file
    if Path(vidinfo.error_out_filename).exists():
        os.remove(vidinfo.error_out_filename)
    try:
        subprocess.run([
            'ffmpeg', '-ss', str(vidinfo.start_time), '-to', str(vidinfo.end_time),
            '-i', video_url, '-pix_fmt', 'yuv420p',
            '-y', vidinfo.video_out_filename
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

        # if vidinfo.download_audio:
        #     subprocess.run([
        #         'ffmpeg', '-ss', str(vidinfo.start_time), '-to', str(vidinfo.end_time),
        #         '-i', video_url, '-q:a', '0', '-map', 'a',
        #         '-y', vidinfo.audio_out_filename
        #     ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

    except subprocess.CalledProcessError:
        return_msg = f'{vidinfo.yt_id}, ERROR (ffmpeg)!'
        return return_msg,vidinfo

    return_msg = f'{vidinfo.yt_id}, DONE!'
    return return_msg,vidinfo
def get_width_with_ffprobe(file_path):
    cmd = [
        'ffprobe', '-v', 'error',
        '-show_entries', 'stream=width',
        '-of', 'json', file_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print("Error running ffprobe:", result.stderr)
        return None
    info = json.loads(result.stdout)
    try:
        width = info['streams'][0]['width']
        
        return width
    except (KeyError, IndexError):
        print("Width information is not available.")
        return None
    
def get_total_bitrate_with_ffprobe(file_path):
    cmd = [
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=bit_rate',
        '-of', 'json', file_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print("Error running ffprobe:", result.stderr)
        return None
    info = json.loads(result.stdout)
    try:
        total_bitrate = info['format']['bit_rate']
        
        return int(total_bitrate)//1000
    except (KeyError, IndexError):
        print("Total bitrate information is not available.")
        return None

    
if __name__ == '__main__':

    if "--csv" not in sys.argv:
        print("csv path was not found")
        sys.exit(1)
    
    csv_file = sys.argv[sys.argv.index("--csv")+1]
    if not os.path.exists(csv_file):
        print("csv path error")
        sys.exit(1)



    
    download_audio = False

    video_format = sys.argv[sys.argv.index("--format") + 1] if "--format" in sys.argv else 'best'

     

    os.makedirs('data', exist_ok=True)
    output_dir = "/archive/torrie/datasets/AVspeech/dataTrainNew"
    with open(csv_file, 'r') as f:
        lines = f.readlines()
        lines = [x.split(',') for x in lines if len(x.split(',')) >= 3]
        # removeDoneLines = []
        # for x in tqdm(lines):
        #     if not (os.path.exists(os.path.join(os.path.join(output_dir, str(x[0])), f"{x[0]}_{x[1]}_{x[2]}_video.mp4")) or os.path.exists(os.path.join(os.path.join(output_dir, str(x[0])), f"{x[0]}_{x[1]}_{x[2]}_error.txt"))) and os.path.exists(os.path.join(os.path.join(output_dir, str(x[0])), f"{x[0]}_{x[1]}_{x[2]}_video.mp4")):
        #         removeDoneLines.append(x)
        #         tqdm.write(f"Kept: {len(removeDoneLines)}")

        
        vidinfos = [VidInfo(x[0], x[1], x[2], "/archive/torrie/datasets/AVspeech/dataTrainNew", download_audio, video_format,"/archive/torrie/datasets/AVspeech/dataTrain") for x in lines]
												
        vidinfos = [x for x in tqdm(vidinfos) if not Path(x.video_out_filename).exists()and not Path(x.done_out_filename).exists() and not Path(x.error_out_filename).exists()]
										 		
        print("Total Videos to download : ", len(vidinfos))
    while True:
        results = ThreadPool(32).imap_unordered(download, vidinfos) 
        from tqdm import tqdm
        loop = tqdm(total=len(vidinfos), position=0, leave=False)
        cnt,err_cnt = 0,0
        notEnglish = 0
        botFailed = []
        
        for r,vidInfo in results:
            cnt += 1
            # print(cnt, '/', len(vidinfos), r)
            loop.set_description(f"Errors: {err_cnt/cnt*100:.2f}%, Bot: {notEnglish/cnt*100:.2f}%, {r}")
            loop.update(1)
            if 'ERROR' in r and 'bot' not in r:
                err_cnt += 1
            elif 'bot' in r:
                notEnglish += 1
                botFailed.append(vidInfo)
        loop.close()
        if len(botFailed) == 0:
            break
        vidinfos = botFailed
        print("Total Error : ",err_cnt)
