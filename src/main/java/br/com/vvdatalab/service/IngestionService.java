package br.com.vvdatalab.service;

import br.com.vvdatalab.dto.IngestionParams;

public interface IngestionService {
	public void executeIngestion(IngestionParams params);
}
