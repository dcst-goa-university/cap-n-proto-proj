@0xe3882ed80287ce18;

struct SensorData {
  timestamp @0 :Float64;
  value @1 :Int32;
}

struct SensorBatch {
  records @0 :List(SensorData);
}

interface SensorService {
  sendData @0 (data :List(SensorData)) -> ();
}
