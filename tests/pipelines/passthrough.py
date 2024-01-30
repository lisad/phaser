from phaser import Pipeline, Phase

class PassThrough(Pipeline):
    phases = [
        Phase(name="passthrough")
    ]
