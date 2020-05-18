package com.huawei.cloudtable.geomesa.examples;

public class TestMain {
	public static void main(String[] args) {
		GeoMesaSample geoMesaSample = new GeoMesaSample();
		
		geoMesaSample.createSchema();
		geoMesaSample.insertFeatures();
		geoMesaSample.queryFeatures();
	}
}
