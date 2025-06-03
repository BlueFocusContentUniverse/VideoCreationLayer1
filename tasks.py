from celery import Celery
import ffmpeg
import logging
import os
from logging.handlers import RotatingFileHandler
# from moviepy import *
import tempfile
import boto3
from pathlib import Path
from contextlib import contextmanager
from moviepy.editor import VideoFileClip, concatenate_videoclips,AudioFileClip, CompositeVideoClip


# 创建 logs 目录
if not os.path.exists('logs'):
    os.makedirs('logs')

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 文件处理器 - 带自动轮转功能，每个文件最大 10MB，保留 5个备份
file_handler = RotatingFileHandler(
    'logs/video_converter.log',
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
))

# 控制台处理器
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'
))

# 添加处理器
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# app = Celery('tasks',
#     broker='redis://localhost:6379/0',
#     backend='redis://localhost:6379/1'
# )

#测试用
app = Celery('tasks')
app.conf.update(
    broker_url='memory://',
    result_backend='rpc://',  # 改用 rpc backend
    task_ignore_result=False,
    task_always_eager=True,  # 强制立即执行任务
    worker_pool='solo'  # 使用单进程模式
)

class S3FileHandler:
    def __init__(self, bucket_name, aws_access_key_id=None, aws_secret_access_key=None):
        """初始化 S3 处理器"""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self.bucket = bucket_name

    @contextmanager
    def download_process_upload(self, s3_input_path, s3_output_path, keep_local=False):
        """
        上下文管理器：下载、处理、上传文件并清理
        :param s3_input_path: S3 输入文件路径
        :param s3_output_path: S3 输出文件路径
        :param keep_local: 是否保留本地临时文件
        """
        # 创建临时目录
        temp_dir = tempfile.mkdtemp(prefix='s3_task_')
        input_path = None
        output_path = None
        
        try:
            # 下载文件
            filename = Path(s3_input_path).name
            input_path = os.path.join(temp_dir, filename)
            logger.info(f"下载文件: {s3_input_path} -> {input_path}")
            
            self.s3_client.download_file(
                self.bucket, 
                s3_input_path, 
                input_path
            )
            
            # 准备输出路径
            output_filename = Path(s3_output_path).name
            output_path = os.path.join(temp_dir, output_filename)
            
            # 提供文件路径给处理函数
            yield input_path, output_path
            
            # 上传处理结果
            if os.path.exists(output_path):
                logger.info(f"上传文件: {output_path} -> {s3_output_path}")
                self.s3_client.upload_file(
                    output_path,
                    self.bucket,
                    s3_output_path
                )
            else:
                raise FileNotFoundError(f"处理后的文件不存在: {output_path}")
                
        finally:
            # 清理临时文件
            if not keep_local:
                try:
                    if input_path and os.path.exists(input_path):
                        os.remove(input_path)
                    if output_path and os.path.exists(output_path):
                        os.remove(output_path)
                    os.rmdir(temp_dir)
                    logger.info(f"清理临时目录: {temp_dir}")
                except Exception as e:
                    logger.error(f"清理临时文件失败: {str(e)}")

@app.task
def atomic_transcode(input_path, output_path, target_format):
    #视频类型转换
    logger.info(f"开始处理视频转换任务: {input_path} -> {output_path}")
    try:
        # 检查输入文件是否存在
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"输入文件不存在: {input_path}")
            
        # 检查输出目录是否存在
        # output_dir = os.path.dirname(output_path)
        # if not os.path.exists(output_dir):
        #     logger.warning(f"输出目录不存在，正在创建: {output_dir}")
        #     os.makedirs(output_dir)
            
        # 视频转换
        stream = ffmpeg.input(input_path)
        stream = ffmpeg.output(stream, output_path,
            format=target_format,
            video_bitrate='5000k',
            qscale=1,
            acodec='copy'
        )
        logger.info("开始转换...")
        ffmpeg.run(stream)
        
        # 检查输出文件
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # 转换为 MB
            logger.info(f"转换成功！输出文件大小: {file_size:.2f}MB")
            return "转换完成"
            
    except FileNotFoundError as e:
        logger.error(f"文件错误: {str(e)}")
        raise
    except ffmpeg.Error as e:
        logger.error(f"FFmpeg 处理错误: {str(e)}")
        logger.debug(f"FFmpeg 错误详情: {e.stderr.decode() if e.stderr else 'No details'}")
        raise
    except Exception as e:
        logger.error(f"未预期的错误: {str(e)}", exc_info=True)
        raise

@app.task
def atomic_resize_video(input_path, output_path, width=None, height=None, keep_aspect_ratio=True):
    """
    调整视频尺寸
    :param input_path: 输入视频路径
    :param output_path: 输出视频路径
    :param width: 目标宽度
    :param height: 目标高度
    :param keep_aspect_ratio: 是否保持原始比例
    """
    try:
        logger.info(f"开始处理视频: {input_path}")
        
        # 获取原始视频信息
        probe = ffmpeg.probe(input_path)
        video_info = next(s for s in probe['streams'] if s['codec_type'] == 'video')
        original_width = int(video_info['width'])
        original_height = int(video_info['height'])
        
        # 计算新尺寸
        if keep_aspect_ratio:
            if width:
                height = int(width * original_height / original_width)
            elif height:
                width = int(height * original_width / original_height)
        
        # 构建转换命令
        stream = ffmpeg.input(input_path)
        stream = ffmpeg.output(stream, output_path,
            vf=f'scale={width}:{height}',
            acodec='copy'  # 保持原始音频
        )
        
        logger.info(f"调整尺寸: {original_width}x{original_height} -> {width}x{height}")
        ffmpeg.run(stream)
        
        logger.info("视频处理完成！")
        return True
        
    except Exception as e:
        logger.error(f"处理失败: {str(e)}")
        raise

@app.task
def atomic_get_media_info(video_path):
    #获取视频元数据
    try:
        logger.info(f"开始读取视频元数据: {video_path}")
        
        # 获取视频信息
        probe = ffmpeg.probe(video_path)
        video_info = next(s for s in probe['streams'] if s['codec_type'] == 'video')
        
        # 提取关键信息
        metadata = {
            '时长': probe['format'].get('duration', 'unknown'),
            '分辨率': f"{video_info.get('width', 'unknown')}x{video_info.get('height', 'unknown')}",
            '编码格式': video_info.get('codec_name', 'unknown'),
            '比特率': probe['format'].get('bit_rate', 'unknown'),
            '文件大小': f"{float(probe['format'].get('size', 0))/1024/1024:.2f} MB"
        }
        
        logger.info(f"元数据获取成功: {metadata}")
        return metadata
        
    except Exception as e:
        logger.error(f"获取元数据失败: {str(e)}")
        raise


def parse_time(time_str):
    """
    解析多种时间格式
    支持: 
    - 秒数: "120"
    - 时:分:秒 "00:02:00"
    - 分:秒 "2:00"
    """
    try:
        # 尝试直接转换为浮点数（秒）
        return float(time_str)
    except ValueError:
        try:
            # 尝试解析 HH:MM:SS 或 MM:SS 格式
            parts = time_str.split(':')
            if len(parts) == 3:
                h, m, s = map(float, parts)
                return h * 3600 + m * 60 + s
            elif len(parts) == 2:
                m, s = map(float, parts)
                return m * 60 + s
        except:
            raise ValueError("不支持的时间格式。请使用 秒数、 MM:SS 或 HH:MM:SS 格式")
@app.task
def atomic_trim_video(input_path, output_path, start_time, end_time):
    """
    裁剪视频
    :param input_path: 输入视频路径
    :param output_path: 输出视频路径
    :param start_time: 开始时间
    :param end_time: 结束时间
    """
    try:
        logger.info(f"开始处理视频: {input_path}")
        logger.info(f"裁剪区间: {start_time} -> {end_time}")

        # 转换时间格式
        start_seconds = parse_time(start_time)
        end_seconds = parse_time(end_time)

        # 加载视频
        video = VideoFileClip(input_path)
        
        # 验证时间范围
        if end_seconds > video.duration:
            logger.warning(f"结束时间超出视频长度，将使用视频结尾作为终点")
            end_seconds = video.duration

        # 裁剪视频
        video = video.subclip(start_seconds, end_seconds)
        
        # 保存
        video.write_videofile(
            output_path,
            codec='libx264',
            audio_codec='aac'
        )
        
        # 清理
        video.close()
        
        logger.info(f"视频裁剪完成：{output_path}")
        return True

    except Exception as e:
        logger.error(f"处理失败: {str(e)}")
        raise
    finally:
        try:
            video.close()
        except:
            pass



def check_video_compatibility(video_paths):
    """检查视频列表是否兼容"""
    try:
        reference_video = VideoFileClip(video_paths[0])
        ref_width = reference_video.w
        ref_height = reference_video.h
        reference_video.close()
        
        logger.info(f"参考视频规格: {ref_width}x{ref_height}")
        
        for path in video_paths[1:]:
            video = VideoFileClip(path)
            if video.w != ref_width or video.h != ref_height:
                video.close()
                raise ValueError(f"视频 {path} 的分辨率 ({video.w}x{video.h}) 与参考视频不匹配")
            video.close()
        return True
            
    except Exception as e:
        logger.error(f"视频兼容性检查失败: {str(e)}")
        raise
@app.task
def atomic_concatenate_videos(video_paths, output_path):
    """
    拼接视频列表
    :param video_paths: 输入视频路径列表
    :param output_path: 输出视频路径
    """
    clips = []
    try:
        logger.info(f"开始处理 {len(video_paths)} 个视频文件")
        
        # 检查视频兼容性
        check_video_compatibility(video_paths)
        
        # 加载视频
        for path in video_paths:
            clip = VideoFileClip(path)
            clips.append(clip)
            logger.info(f"已加载视频: {path}, 时长: {clip.duration}秒")
        
        # 拼接视频
        final_clip = concatenate_videoclips(clips)
        
        # 保存
        final_clip.write_videofile(
            output_path,
            codec='libx264',
            audio_codec='aac'
        )
        
        logger.info(f"视频拼接完成: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"视频拼接失败: {str(e)}")
        raise
    finally:
        # 清理资源
        for clip in clips:
            try:
                clip.close()
            except:
                pass


@app.task
def atomic_add_audio(
    video_path, 
    audio_path, 
    output_path, 
    volume=1.0,
    loop_audio=False,
    keep_original_audio=False,
    original_audio_volume=0.0
):
    """
    为视频添加或替换音轨
    :param video_path: 视频文件路径
    :param audio_path: 音频文件路径
    :param output_path: 输出文件路径
    :param volume: 新音轨音量 (1.0 = 100%)
    :param loop_audio: 是否循环音频
    :param keep_original_audio: 是否保留原始音频
    :param original_audio_volume: 原始音频音量
    """
    video_clip = None
    audio_clip = None
    
    try:
        # 加载视频和音频
        logger.info(f"加载视频: {video_path}")
        video_clip = VideoFileClip(video_path)
        
        logger.info(f"加载音频: {audio_path}")
        audio_clip = AudioFileClip(audio_path)
        
        # 处理音频循环
        if loop_audio and audio_clip.duration < video_clip.duration:
            logger.info("音频时长不足，启用循环")
            repeats = int(video_clip.duration / audio_clip.duration) + 1
            audio_clip = audio_clip.loop(repeats)
        
        # 裁剪音频到视频长度
        audio_clip = audio_clip.subclip(0, video_clip.duration)
        
        # 设置新音轨音量
        audio_clip = audio_clip.volumex(volume)
        
        # 处理最终音频
        if keep_original_audio and video_clip.audio is not None:
            logger.info("合并原始音频和新音频")
            original_audio = video_clip.audio.volumex(original_audio_volume)
            final_audio = CompositeVideoClip([
                video_clip.set_audio(original_audio),
                video_clip.set_audio(audio_clip)
            ]).audio
        else:
            final_audio = audio_clip
        
        # 创建最终视频
        final_video = video_clip.set_audio(final_audio)
        
        # 导出视频
        logger.info(f"开始导出视频: {output_path}")
        final_video.write_videofile(
            output_path,
            codec='libx264',
            audio_codec='aac'
        )
        
        logger.info("处理完成！")
        return True
        
    except Exception as e:
        logger.error(f"处理失败: {str(e)}")
        raise
        
    finally:
        # 清理资源
        if video_clip: video_clip.close()
        if audio_clip: audio_clip.close()
