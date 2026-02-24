import os
import google.generativeai as genai
import multiprocessing as mp
from typing import Union, List, Dict, Optional,Any
import time


class Gemini:
    def __init__(
        self, 
        api_key: str,
        model_name: str = "gemini-2.0-flash", 
        temperature: float = 0.0,
        top_p: float = 0.95,
        top_k: int = 40,
        max_output_tokens: int = 8192
    ):
        """
        Initialize the Gemini AI class with configurable parameters.
        
        Args:
            model_name (str, optional): Name of the Gemini model to use. Defaults to "gemini-2.0-flash".
            temperature (float, optional): Controls randomness in generation. Defaults to 0.0.
            top_p (float, optional): Nucleus sampling parameter. Defaults to 0.95.
            top_k (int, optional): Top-k sampling parameter. Defaults to 40.
            max_output_tokens (int, optional): Maximum number of tokens to generate. Defaults to 8192.
        """
        # Configure the generative AI API with the API key
        self.api_key = api_key
        if not self.api_key:
            raise ValueError("Gemini API key not found. Please set the 'gemini_key' environment variable.")
        
        genai.configure(api_key=self.api_key)
        
        # Store model configuration
        self.model_name = model_name
        self.default_config = {
            "temperature": temperature,
            "top_p": top_p,
            "top_k": top_k,
            "max_output_tokens": max_output_tokens
        }
    
    def _generate_single_response(
        self, 
        context: str, 
        system_prompt: Optional[str] = None, 
        model_params: Optional[Dict] = None
    ) -> str:
        """
        Generate a response for a single context input.
        
        Args:
            context (str): The main query or context.
            system_prompt (str, optional): System-level instructions for the model.
            model_params (dict, optional): Additional model generation parameters.
        
        Returns:
            str: Generated response from the model.
        """
        # Prepare generation configuration
        generation_config = self.default_config.copy()
        
        # Update with user-provided model parameters if any
        if model_params:
            generation_config.update({
                k: v for k, v in model_params.items() 
                if k in generation_config
            })
        
        # Prepare full prompt
        full_prompt = system_prompt + "\n" + context if system_prompt else context
        
        # Create model with configuration
        model = genai.GenerativeModel(
            model_name=self.model_name,
            generation_config=genai.types.GenerationConfig(**generation_config)
        )
        
        # Generate and return response
        response = model.generate_content(full_prompt)
        return response.text
    
    def generate_text(
        self, 
        context: Union[str, List[str]], 
        system_prompt: Optional[str] = None, 
        model_params: Optional[Dict] = None,
        max_workers: int = 10
    ) -> Union[str, List[str]]:
        """
        Generate text for given context(s) with optional system prompt and model parameters.
        
        Args:
            context (str or List[str]): Input context or list of contexts.
            system_prompt (str, optional): System-level instructions for the model.
            model_params (dict, optional): Additional model generation parameters.
            max_workers (int, optional): Maximum number of parallel processes. Defaults to 10.
        
        Returns:
            str or List[str]: Generated response(s) matching input context type.
        """
        # Handle single context input
        if isinstance(context, str):
            return self._generate_single_response(
                context, 
                system_prompt, 
                model_params
            )
        
        # Handle list of contexts with parallel processing
        if not context:
            return []
        
        # Divide contexts into batches of max_workers
        def process_batch(batch):
            with mp.Pool(processes=min(max_workers, len(batch))) as pool:
                return pool.starmap(
                    self._generate_single_response, 
                    [(ctx, system_prompt, model_params) for ctx in batch]
                )
        
        # Process contexts in batches
        results = []
        for i in range(0, len(context), max_workers):
            batch = context[i:i+max_workers]
            results.extend(process_batch(batch))
        
        return results


class OpenAI:
    def __init__(
        self, 
        model: str = "gpt-4o-mini", 
        temperature: float = 0.0,
        top_p: float = 0.95,
        top_k: int = 40,
        max_output_tokens: int = 8192
    ):
        """
        Initialize the OpenAI API wrapper with configurable parameters.
        
        :param model: OpenAI model to use (default: gpt-4o-mini)
        :param temperature: Sampling temperature (default: 0.0)
        :param top_p: Nucleus sampling probability threshold (default: 0.95)
        :param top_k: Top-k sampling parameter (default: 40)
        :param max_output_tokens: Maximum number of tokens to generate (default: 8192)
        """
        # Ensure API key is set
        if 'open_ai_key' not in os.environ:
            raise ValueError("OpenAI API key must be set in environment variable 'open_ai_key'")
        
        openai.api_key = os.getenv('open_ai_key')
        
        # Store default model parameters
        self.default_params = {
            "model": model,
            "temperature": temperature,
            "top_p": top_p,
            "top_k": top_k,
            "max_tokens": max_output_tokens
        }

    def _generate_single_text(
        self, 
        context: str, 
        system_prompt: str, 
        model_params: Dict[str, Any] = None
    ) -> str:
        """
        Generate text for a single context.
        
        :param context: Input context for text generation
        :param system_prompt: System prompt to guide model behavior
        :param model_params: Additional model parameters
        :return: Generated text
        """
        # Merge default parameters with user-provided parameters
        params = self.default_params.copy()
        if model_params:
            params.update(model_params)
        
        # Remove unsupported parameters
        params.pop('top_k', None)
        if 'max_new_tokens' in params:
            params['max_tokens'] = params.pop('max_new_tokens')
        
        # Prepare messages
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": context}
        ]
        
        # Create completion
        completion = openai.chat.completions.create(
            model=params.pop('model'),
            messages=messages,
            **params
        )
        
        return completion.choices[0].message.content

    def generate_text(
        self, 
        context: Union[str, List[str]], 
        system_prompt: str = "You are a helpful assistant.", 
        model_params: Dict[str, Any] = None,
        max_workers: int = 10
    ) -> Union[str, List[str]]:
        """
        Generate text for given context(s) with optional parallel processing.
        
        :param context: Single context or list of contexts
        :param system_prompt: System prompt to guide model behavior
        :param model_params: Additional model parameters
        :param max_workers: Maximum number of parallel workers (default: 10)
        :return: Generated text or list of generated texts
        """
        # If context is a single string, process and return single result
        if isinstance(context, str):
            return self._generate_single_text(context, system_prompt, model_params)
        
        # If context is a list, process in parallel
        if not isinstance(context, list):
            raise ValueError("Context must be a string or list of strings")
        
        # Divide context into batches of max_workers
        results = []
        for i in range(0, len(context), max_workers):
            batch = context[i:i+max_workers]
            
            # Use multiprocessing to generate texts in parallel
            with mp.Pool(processes=min(len(batch), max_workers)) as pool:
                batch_results = pool.starmap(
                    self._generate_single_text, 
                    [(ctx, system_prompt, model_params) for ctx in batch]
                )
            
            results.extend(batch_results)
        
        return results

# Example usage
if __name__ == "__main__":
    # Initialize Gemini with custom default parameters
    gemini_ai = Gemini(
        model_name="gemini-1.5-pro", 
        temperature=0.7, 
        top_p=0.9
    )
    
    # start = time.time()

    # # Single context example
    # single_response = gemini_ai.generate_text(
    #     context="Write a short poem about AI.",
    #     system_prompt="You are a creative poet."
    # )
    # print("Single Response:", single_response)

    # stop = time.time()
    # print(f'total time elapsed: {stop-start}')
    
    start = time.time()
    # Multiple contexts example
    multi_contexts = [
    "How does artificial intelligence work?",
    "Explain the concept of black holes.",
    "What are the principles of cybersecurity?",
    "Describe the basics of genetic engineering.",
    "What is the Internet of Things (IoT)?",
    "How does cryptocurrency mining work?",
    "Explain the theory of relativity.",
    "What are the different types of cloud computing?",
    "Describe the working of neural networks.",
    "How does nuclear fusion differ from fission?",
    "What is quantum entanglement?",
    "Explain the fundamentals of natural language processing.",
    "How do self-driving cars work?",
    "What are the key concepts of the metaverse?",
    "How does DNA sequencing work?",
    "Explain the basics of renewable energy sources.",
    "What is the Turing test in AI?",
    "Describe the principles of virtual reality.",
    "How do vaccines work?",
    "What are the main components of an operating system?"
]
    multi_responses = gemini_ai.generate_text(
        context=multi_contexts,
        system_prompt="Provide clear and concise explanations."
    )
    print("Multiple Responses:", multi_responses)
    stop = time.time()
    print(f'total time elapsed: {stop-start}')

    ########################################
    open_ai = OpenAI(
        model="gpt-4o-mini",
        temperature=0.7,
        top_p=0.9
    )

    start = time.time()
    contexts = [
        "Write a haiku about spring",
        "Compose a limerick about a mathematician",
        "Create a short poem about technology"
    ]
    multi_responses = open_ai.generate_text(
        context = contexts, 
        system_prompt="You are a skilled poet."
    )
    print("Multiple Contexts Responses:", multi_responses)
    stop = time.time()
    print(f'total time elapsed: {stop-start}')