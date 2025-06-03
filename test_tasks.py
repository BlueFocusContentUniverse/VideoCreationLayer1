import pytest
import os
import tasks
# from moviepy import *
from moviepy.editor import ColorClip, AudioClip, concatenate_videoclips

# 生成测试视频
@pytest.fixture
def test_video(tmp_path):
    path = tmp_path / "test.mp4"
    clip = ColorClip(size=(320, 240), color=(255, 0, 0), duration=2)
    clip.write_videofile(str(path), fps=24, codec="libx264", audio=False)
    clip.close()
    return str(path)

# 生成测试音频
@pytest.fixture
def test_audio(tmp_path):
    path = tmp_path / "test.wav"
    # 生成2秒440Hz正弦波
    def make_frame(t):
        import numpy as np
        return [0.5 * np.sin(2 * np.pi * 440 * t)]
    audio = AudioClip(make_frame, duration=2, fps=44100)
    audio.write_audiofile(str(path))
    audio.close()
    return str(path)

def test_atomic_transcode(tmp_path, test_video):
    output = tmp_path / "out.webm"
    result = tasks.atomic_transcode.run(test_video, str(output), "webm")
    assert os.path.exists(output)
    assert result == "转换完成"

def test_atomic_resize_video(tmp_path, test_video):
    output = tmp_path / "resized.mp4"
    result = tasks.atomic_resize_video.run(test_video, str(output), width=160, height=120)
    assert os.path.exists(output)
    assert result is True

def test_atomic_get_media_info(test_video):
    info = tasks.atomic_get_media_info.run(test_video)
    assert "时长" in info
    assert "分辨率" in info
    assert "编码格式" in info

def test_atomic_trim_video(tmp_path, test_video):
    output = tmp_path / "trimmed.mp4"
    result = tasks.atomic_trim_video.run(test_video, str(output), "0", "1")
    assert os.path.exists(output)
    assert result is True

def test_atomic_concatenate_videos(tmp_path, test_video):
    # 复制两份
    video2 = tmp_path / "test2.mp4"
    os.system(f"cp {test_video} {video2}")
    output = tmp_path / "concat.mp4"
    result = tasks.atomic_concatenate_videos.run([test_video, str(video2)], str(output))
    assert os.path.exists(output)
    assert result is True

def test_atomic_add_audio(tmp_path, test_video, test_audio):
    output = tmp_path / "video_with_audio.mp4"
    result = tasks.atomic_add_audio.run(
        test_video, test_audio, str(output),
        volume=1.0, loop_audio=False, keep_original_audio=False
    )
    assert os.path.exists(output)
    assert result is True