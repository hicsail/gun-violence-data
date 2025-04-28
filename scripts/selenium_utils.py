from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.remote.webdriver import WebDriver
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

def exists_element(self, by, value):    
    try:
        self.find_element(by, value)
    except NoSuchElementException:        
        return False;       
    return True;

def click(self, element):
    script = 'arguments[0].scrollIntoView();'
    self.execute_script(script, element)
    element.click()

def find_element_or_wait(self, by, value, ancestor=None, timeout=60):
    
    ancestor = ancestor or self
    try:
        # Common case: the element is already loaded and we don't need to wait.
        return ancestor.find_element(by, value)
    except NoSuchElementException:
        try:
            wait = WebDriverWait(self, timeout)
            wait.until(EC.visibility_of_element_located((by, value)))
        except TimeoutException:
            # Let the next find_element() call throw a NoSuchElementException.
            pass
        return ancestor.find_element(by, value)

def find_elements_or_wait(self, by, value, ancestor=None, timeout=10):
    ancestor = ancestor or self
    try:
        # Common case: the elements are already loaded and we don't need to wait.
        return ancestor.find_elements(by, value)
    except NoSuchElementException:
        try:
            wait = WebDriverWait(self, timeout)
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, value)))
        except TimeoutException:
            # Let the next find_elements() call throw a NoSuchElementException.
            pass
        return ancestor.find_elements(by, value)

def get_value(self, element, decode=True):
    property = 'innerText' if decode else 'innerHTML'
    script = 'return arguments[0].{};'.format(property)
    return self.execute_script(script, element)

def has_page_loaded(self):
    script = 'return document.readyState;'
    return self.execute_script(script) == 'complete'

# TODO: Use getattr/setattr and __all__ instead of writing things out by hand.
options = webdriver.ChromeOptions()
#options.add_experimental_option('w3c', False)
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
#webdriver = webdriver.Chrome(options=options)
#webdriver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

# Create a Service object for ChromeDriver
service = Service(ChromeDriverManager().install())

# Initialize the Chrome driver with the Service and options
driver = webdriver.Chrome(service=service, options=options)

WebDriver.click = click
WebDriver.find_element_or_wait = find_element_or_wait
WebDriver.find_elements_or_wait = find_elements_or_wait
WebDriver.get_value = get_value
WebDriver.has_page_loaded = has_page_loaded
WebDriver.exists_element = exists_element