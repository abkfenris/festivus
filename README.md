# festivus

[festivus](https://arxiv.org/pdf/1702.03935.pdf) for the rest of us. [A video](https://www.youtube.com/watch?v=2Kmmbs2xnZc)

[Descartes Labs](http://descarteslabs.com) has built a really cool FUSE filesystem for Google Cloud Storage. The usual issue with the GCS and S3 Fuse systesms is that the metadata access is incredibly slow compared to what the system expects. Descartes bypassed that by having the metadata served from a Redis DB.

## If you use this in production it is your own damn fault when it bites you!

<small>_Though I'm really interested in how you managed that_</small>