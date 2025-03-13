import os
import subprocess
import time
import pika
import signal
import sys
import json
import traceback
from gradio_client import Client, handle_file
from minio import Minio
from minio.error import S3Error
import requests

QUEUE_NAME = "task_queue"
RABBITMQ_URL = "amqp://admin:admin@192.168.2.133"
DLX_EXCHANGE = "dlx_exchange"
DLX_QUEUE = "dlx_queue"


# 连接 RabbitMQ 的函数（带重试）
def connect():
    while True:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            print("✅ 已成功连接 RabbitMQ")
            return connection
        except Exception as e:
            print(f"❌ 连接 RabbitMQ 失败: {e}")
            print("⏳ 5 秒后重试...")
            time.sleep(5)  # 5 秒后重试


# 启动 RabbitMQ 消费者
def connectRabbitmq():
    while True:  # 断线后自动重连
        try:
            connection = connect()
            channel = connection.channel()

            # 声明死信交换机和队列
            channel.exchange_declare(
                exchange=DLX_EXCHANGE, exchange_type="direct", durable=True
            )
            channel.queue_declare(queue=DLX_QUEUE, durable=True)
            channel.queue_bind(
                queue=DLX_QUEUE, exchange=DLX_EXCHANGE, routing_key="retry"
            )

            # 声明业务队列，绑定死信队列
            channel.queue_declare(
                queue=QUEUE_NAME,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": DLX_EXCHANGE,
                    "x-dead-letter-routing-key": "retry",
                },
            )
            print(f'✅ 队列 "{QUEUE_NAME}" 已就绪，等待任务...')

            # 处理消息的回调
            def callback(ch, method, properties, body):
                try:
                    print(f"📥 处理任务: {body.decode()}")
                    message = json.loads(body.decode())  # 解析 JSON
                    # result = True
                    result = consumeAndTrain(message)  # 处理任务
                    if result:
                        print(f"✅ 任务处理成功: {body.decode()}")
                        ch.basic_ack(delivery_tag=method.delivery_tag)  # 确认
                    else:
                        print(f"❌ 任务处理失败: {body.decode()}")
                        ch.basic_nack(
                            delivery_tag=method.delivery_tag, requeue=False
                        )  # 丢弃
                except Exception as e:
                    print(f"❌ 处理任务时出错: {e}")
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag, requeue=False
                    )  # 丢弃

            # 监听队列
            channel.basic_consume(
                queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False
            )

            # 监听进程终止信号
            def graceful_exit(signum, frame):
                print("\n🔄 收到终止信号，正在关闭连接...")
                channel.close()
                connection.close()
                print("👋 连接已关闭")
                sys.exit(0)

            signal.signal(signal.SIGINT, graceful_exit)
            signal.signal(signal.SIGTERM, graceful_exit)

            print(" [*] 等待消息，按 Ctrl+C 退出")
            channel.start_consuming()

        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.ChannelClosedByBroker,
        ) as e:
            print(f"⚠️ 连接断开: {e}，正在重连...")
            time.sleep(5)  # 等待 5 秒后重连
        except Exception as e:
            print(f"❌ 发生错误: {e}")
            print(traceback.format_exc())
            time.sleep(5)


# 根据rabbit的信息进行lora消费和训练
def consumeAndTrain(config):
    print(f"💎正在执行训练任务: {config}")
    client = Client("http://127.0.0.1:7860/")

    # 获取样本
    print("*************************获取样本*************************")
    client.predict(lora_name=config["lora_name"], api_name="/get_samples")

    # 更新样本
    print("*************************更新样本*************************")
    client.predict(concept_sentence=config["lora_name"], api_name="/update_sample")

    # 加载标注
    print("*************************加载标注*************************")
    client.predict(
        uploaded_files=[handle_file(file) for file in config["uploaded_files"]],
        concept_sentence=config["lora_name"],
        api_name="/load_captioning",
    )

    # 更新总步数
    print("*************************更新总步数*************************")
    client.predict(
        max_train_epochs=config["max_train_epochs"],
        num_repeats=config["num_repeats"],
        images=[handle_file(file) for file in config["uploaded_files"]],
        api_name="/update_total_steps_2",
    )

    # 更新
    print("*************************更新配置信息*************************")
    client.predict(
        base_model="flux-dev",
        lora_name=config["lora_name"],
        resolution=512,
        seed=42,
        workers=2,
        class_tokens=config["lora_name"],
        learning_rate="8e-4",
        network_dim=4,
        max_train_epochs=config["max_train_epochs"],
        save_every_n_epochs=4,
        timestep_sampling="shift",
        guidance_scale=1,
        vram=config["vram"],
        num_repeats=config["num_repeats"],
        sample_prompts=config["lora_name"],
        sample_every_n_steps=0,
        param_16="",
        param_17=False,
        param_18=False,
        param_19=False,
        param_20="",
        param_21="",
        param_22=False,
        param_23="",
        param_24=False,
        param_25=False,
        param_26="",
        param_27="",
        param_28="",
        param_29="",
        param_30="",
        param_31="",
        param_32="",
        param_33="",
        param_34="",
        param_35=False,
        param_36="",
        param_37="",
        param_38="",
        param_39="",
        param_40=False,
        param_41="",
        param_42=False,
        param_43="",
        param_44="",
        param_45=False,
        param_46=False,
        param_47="",
        param_48=False,
        param_49=False,
        param_50=False,
        param_51=False,
        param_52="",
        param_53=False,
        param_54=False,
        param_55="",
        param_56=False,
        param_57=False,
        param_58=False,
        param_59=False,
        param_60=False,
        param_61=False,
        param_62="",
        param_63="",
        param_64="",
        param_65="",
        param_66="",
        param_67="",
        param_68="",
        param_69="",
        param_70="",
        param_71="",
        param_72="",
        param_73="",
        param_74="",
        param_75=False,
        param_76="",
        param_77="",
        param_78=False,
        param_79="",
        param_80="",
        param_81="",
        param_82="",
        param_83="",
        param_84=False,
        param_85="",
        param_86="",
        param_87="",
        param_88="",
        param_89="",
        param_90="",
        param_91="",
        param_92="",
        param_93=False,
        param_94="",
        param_95="",
        param_96="",
        param_97="",
        param_98="",
        param_99=False,
        param_100="",
        param_101="",
        param_102="",
        param_103="",
        param_104="",
        param_105="",
        param_106="",
        param_107="",
        param_108="",
        param_109="",
        param_110="",
        param_111="",
        param_112=False,
        param_113=False,
        param_114="",
        param_115=False,
        param_116=False,
        param_117="",
        param_118=False,
        param_119="",
        param_120="",
        param_121="",
        param_122="",
        param_123=False,
        param_124="",
        param_125=False,
        param_126="",
        param_127="",
        param_128="",
        param_129=False,
        param_130=False,
        param_131="",
        param_132="",
        param_133="",
        param_134="",
        param_135="",
        param_136="",
        param_137="",
        param_138="",
        param_139=False,
        param_140=False,
        param_141=False,
        param_142=False,
        param_143="",
        param_144="",
        param_145=False,
        param_146="",
        param_147=False,
        param_148=False,
        param_149="",
        param_150="",
        param_151="",
        param_152="",
        param_153="",
        param_154=False,
        param_155="",
        param_156="",
        param_157="",
        param_158="",
        param_159=False,
        param_160=False,
        param_161=False,
        param_162=False,
        param_163="",
        param_164="",
        param_165="",
        param_166="",
        param_167="",
        param_168="",
        param_169="",
        param_170="",
        param_171="",
        param_172=False,
        param_173=False,
        param_174=False,
        param_175=False,
        param_176="",
        param_177=False,
        api_name="/update",
    )

    # 运行标注
    print("*************************运行标注*************************")
    num_uploaded_files = len(config["uploaded_files"])
    params = {
        f"param_{i}": config["lora_name"] if i - 2 < num_uploaded_files else ""
        for i in range(2, 152)
    }
    params.update(
        {
            "images": [handle_file(file) for file in config["uploaded_files"]],
            "concept_sentence": config["lora_name"],
            "api_name": "/run_captioning",
        }
    )
    captions = client.predict(**params)

    # 创建数据集
    print("*************************创建数据集*************************")
    params = {
        f"param_{i}": captions[i - 3] if i - 3 < 150 else "" for i in range(3, 153)
    }
    params.update(
        {
            "size": 512,
            "param_2": [handle_file(file) for file in config["uploaded_files"]],
            "api_name": "/create_dataset",
        }
    )
    client.predict(**params)

    # 开始训练lora
    print("*************************开始训练lora*************************")
    client.predict(
        base_model="flux-dev",
        lora_name=config["lora_name"],
        train_script=f"""accelerate launch \
    --mixed_precision bf16 \
    --num_cpu_threads_per_process 1 \
    sd-scripts/flux_train_network.py \
    --pretrained_model_name_or_path "/home/eblaz/tietiezhi/FluxTrainer/models/unet/flux1-dev.sft" \
    --clip_l "/home/eblaz/tietiezhi/FluxTrainer/models/clip/clip_l.safetensors" \
    --t5xxl "/home/eblaz/tietiezhi/FluxTrainer/models/clip/t5xxl_fp16.safetensors" \
    --ae "/home/eblaz/tietiezhi/FluxTrainer/models/vae/ae.sft" \
    --cache_latents_to_disk \
    --save_model_as safetensors \
    --sdpa --persistent_data_loader_workers \
    --max_data_loader_n_workers 2 \
    --seed 42 \
    --gradient_checkpointing \
    --mixed_precision bf16 \
    --save_precision bf16 \
    --network_module networks.lora_flux \
    --network_dim 4 \
    --optimizer_type adamw8bit \
    --learning_rate 8e-4 \
    --cache_text_encoder_outputs \
    --cache_text_encoder_outputs_to_disk \
    --fp8_base \
    --highvram \
    --max_train_epochs {config['max_train_epochs']} \
    --save_every_n_epochs 4 \
    --dataset_config "/home/eblaz/tietiezhi/FluxTrainer/outputs/{config["lora_name"]}/dataset.toml" \
    --output_dir "/home/eblaz/tietiezhi/FluxTrainer/outputs/{config["lora_name"]}" \
    --output_name {config["lora_name"]} \
    --timestep_sampling shift \
    --discrete_flow_shift 3.1582 \
    --model_prediction_type raw \
    --guidance_scale 1 \
    --loss_type l2 """,
        train_config=f"""[general]
    shuffle_caption = false
    caption_extension = '.txt'
    keep_tokens = 1

    [[datasets]]
    resolution = 512
    batch_size = 1
    keep_tokens = 1

    [[datasets.subsets]]
    image_dir = '/home/eblaz/tietiezhi/FluxTrainer/datasets/{config["lora_name"]}'
    class_tokens = '{config["lora_name"]}'
    num_repeats = {config["num_repeats"]}""",
        sample_prompts=config["lora_name"],
        api_name="/start_training",
    )

    return check_lora_safetensors(config)


def check_lora_safetensors(config):
    lora_name = config["lora_name"]
    minio_address = config["minio_address"]
    minio_access_key = config["minio_access_key"]
    minio_secret_key = config["minio_secret_key"]
    minio_bucket = config["minio_bucket"]
    task_id = config["task_id"]

    # MinIO 配置
    minio_client = Minio(
        minio_address,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )

    # 构造文件夹路径
    folder_path = os.path.join("outputs", lora_name)
    safetensors_file = os.path.join(folder_path, f"{lora_name}.safetensors")

    # 检查文件夹和文件是否存在
    if not os.path.isdir(folder_path):
        print(f"❌ 文件夹 {folder_path} 不存在")
        return False

    if not os.path.isfile(safetensors_file):
        print(f"❌ 文件 {safetensors_file} 不存在")
        return False

    print(f"✅ 文件 {safetensors_file} 存在，准备上传...")

    try:
        # 确保存储桶存在
        if not minio_client.bucket_exists(minio_bucket):
            minio_client.make_bucket(minio_bucket)
            print(f"✅ 存储桶 {minio_bucket} 已创建")

        # 上传文件
        object_name = f"{lora_name}.safetensors"
        minio_client.fput_object(minio_bucket, object_name, safetensors_file)
        print(
            f"✅ 文件 {safetensors_file} 成功上传到 MinIO {minio_bucket}/{object_name}"
        )

        # 上传成功后，发送 GET 请求
        lora_url = f"http://{minio_address}/{minio_bucket}/{object_name}"
        params = {"task_id": task_id, "lora_url": lora_url}
        response = requests.get("http://192.168.2.133:1015/callback", params=params)

        if response.status_code == 204:
            print(f"✅ 任务 {task_id} 回调请求成功")
        else:
            print(f"❌ 任务 {task_id} 回调请求失败，状态码: {response.status_code}")
            return False

        return True
    except S3Error as e:
        print(f"❌ 上传失败: {e}")
        return False


if __name__ == "__main__":
    subprocess.Popen(["python", "app.py"])
    connectRabbitmq()
