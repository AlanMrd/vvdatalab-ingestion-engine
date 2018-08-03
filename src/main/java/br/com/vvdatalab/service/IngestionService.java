package br.com.vvdatalab.service;

import java.io.Serializable;

public interface IngestionService extends Serializable {
	public void executeIngestion() throws InstantiationException, IllegalAccessException;
}
