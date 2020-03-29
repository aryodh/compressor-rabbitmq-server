import zipfile, os, requests, types, threading
from flask import Flask, request, render_template, jsonify, send_from_directory
from werkzeug.utils import secure_filename
from functools import partial
import pika

# -------------------------------
# ----------- Setup -------------
# -------------------------------

app = Flask(__name__)
app.config.from_object(__name__)

UPLOAD_FOLDER = './'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# -------------------------------
# ---------- Compress -----------
# -------------------------------

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/compress/', methods=['POST'])
def upload():
    uniqueId = request.headers.get('X-ROUTING-KEY')
    if 'file' not in request.files:
        return {"message":"No file part"}
    
    file = request.files.get('file')
    filename_w_frmt = secure_filename(file.filename)
    filename = filename_w_frmt.replace(" ", "_")
    
    if file.filename == '':
        return {"message":"No selected file"}

    if file and allowed_file(file.filename):
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        threading.Thread(target=compress, args=[filename_w_frmt, filename, uniqueId]).start()

    return {"message":"Error"}

def compress(filename_w_frmt, filename, uniqueId):
        global the_bytes
        global the_obytes
        global perc

        the_bytes = 0
        the_obytes = 0
        perc = [10,20,30,40,50,60,70,80,90,100]
        
        pos_dot = max([pos for pos, char in enumerate(filename) if char == "."])
        filename = filename[:pos_dot]
        zipname = filename + ".zip"

        with zipfile.ZipFile(zipname, 'w', compression=zipfile.ZIP_DEFLATED) as _zip:
            the_bytes = 0
            the_obytes = 0
            # Replace original write() with a wrapper to track progress
            _zip.fp.write = types.MethodType(partial(progress, os.path.getsize(filename_w_frmt), _zip.fp.write, uniqueId, filename), _zip.fp)
            _zip.write(filename_w_frmt)

        
        os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename_w_frmt))
        os.replace("./" + zipname, "./storage/" + zipname)

        return {"message":"Success!", "link":"/download/"+ filename+".zip"}



def progress(total_size, original_write, uniqueId, filename, self, buf):
    global the_bytes
    global the_obytes
    global server
    the_bytes += len(buf)
    the_obytes += 1024 * 8  # Hardcoded in zipfile.write
    percent = int(100 * the_obytes / total_size)
    global perc
    if percent in perc:
        try:
            if len(perc) > 0:
                if len(perc) > 1:
                    perc = perc[1:]
                    server.publish(payload=str("progress " + str(percent)), routing_key=uniqueId)
                    return original_write(buf)
                server.publish(payload="done /download/" + filename, routing_key=uniqueId)
        except:
            server = RabbitMq('upload')
            server.publish(payload=str("progress " + str(percent)), routing_key=uniqueId)
    return original_write(buf)

# -------------------------------
# ---------- Download -----------
# -------------------------------

@app.route('/download/<filename>')
def download(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'] + "storage/", filename)

# -------------------------------
# ---------- RabbitMq -----------
# -------------------------------

class RabbitMq():

    def __init__(self, queue="hello"):

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange='1706039515', exchange_type='direct')

    def publish(self, payload = "", routing_key=""):

        self._channel.basic_publish(exchange="1706039515",
                                    routing_key=routing_key,
                                    body=str(payload))


if __name__ == '__main__':
    global server
    server = RabbitMq('upload')
    app.run(port=20644, threaded=True)
