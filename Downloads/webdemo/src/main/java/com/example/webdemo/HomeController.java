package com.example.webdemo;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import org.yaml.snakeyaml.scanner.Constant;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

@Controller
public class HomeController {

	@RequestMapping("/home")
	public ModelAndView home( Alien alien ){
		ModelAndView mv = new ModelAndView();
		mv.addObject("alien", alien);
		mv.setViewName("home");
		return mv;
	}
}
