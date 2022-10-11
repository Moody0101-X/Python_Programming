"""
only analysis
"""


def download(self, filepath="", quiet=False, progress="Bytes",
                 callback=None, meta=False, remux_audio=False):

        downloader = youtube_dl.downloader.http.HttpFD(ydl(),
            {'http_chunk_size': 10485760})

        progress_available = ["KB", "MB", "GB"]
        if progrprogress.pyproess not in progress_available:
            progress = "Bytes"

        status_string = get_status_string(progress)

        def progress_hook(s):
            if s['status'] == 'downloading':
                bytesdone = s['downloaded_bytes']
                total = s['total_bytes']
                if s.get('speed') is not None:
                    rate = s['speed'] / 1024
                else:
                    rate = 0
                if s.get('eta') is None:
                    eta = 0
                else:
                    eta = s['eta']

                progress_stats = (get_size_done(bytesdone, progress),
                                  bytesdone*1.0/total, rate, eta)
                if not quiet:
                    status = status_string.format(*progress_stats)
                    sys.stdout.write("\r" + status + ' ' * 4 + "\r")
                    sys.stdout.flush()

                if callback:
                    callback(total, *progress_stats)

        downloader._progress_hooks = [progress_hook]

        if filepath and os.path.isdir(filepath):
            filename = self.generate_filename(max_length=256 - len('.temp'))
            filepath = os.path.join(filepath, filename)

        elif filepath:
            pass

        else:
            filepath = self.generate_filename(meta=meta, max_length=256 - len('.temp'))

        infodict = {'url': self.url}

        downloader.download(filepath, infodict)
        print()

        if remux_audio and self.mediatype == "audio":
            subprocess.run(['mv', filepath, filepath + '.temp'])
            remux(filepath + '.temp', filepath, quiet=quiet, muxer=remux_audio)