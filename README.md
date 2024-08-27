# Nexus to Kafka

To test this out, first make sure there is a kafka cluster running locally (it should work with remote clusters too but not tested yet).
By default the producer and consumer assume the kafka cluster is exposed at `localhost:9092`, this can be updated in the config.toml file.
This repo also uses [pixi](https://pixi.sh/latest/) to manage environments and run tasks.
Once you have the repo locally run `pixi install`.

Valid data from CODA needs to be linked from the `data` folder.

To stream all the `ev44` events from a nexus file:

```
pixi run produce --topic nmx_detector --file data/616254_00018152.hdf
```

This is an example of using a NMX nexus file and streaming the `ev44` events to the kafka topic `nmx_detector`.

You can also consume the stream (with deseralising flatbuffers) with:

```
pixi run consume --topic nmx_detector
```

### TODO
- [ ] We can probably pick up the topic directly from the nexus file, instead of being explicit the produce command.
- [ ] Document the get_forward_delta function to explain the timestamp of event production.
- [ ] Use copier template for this instead?
- [ ] Test this out with all instruments




https://github.com/user-attachments/assets/ef5e8f99-7e8a-4ab7-85a0-7f99025845f2

