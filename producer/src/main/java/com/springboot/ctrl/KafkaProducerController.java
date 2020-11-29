package com.springboot.ctrl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.springboot.model.Student;
import com.springboot.service.KafkaSender;

@RestController
@RequestMapping("/kafkaProducer")
public class KafkaProducerController {

	@Autowired
	private KafkaSender sender;
	
	@PostMapping
	public ResponseEntity<String> sendData(@RequestParam String key,
										   @RequestBody Student student){
		sender.sendData(key, student);
		return new ResponseEntity<>("Data sent to Kafka", HttpStatus.OK);
	}
}
