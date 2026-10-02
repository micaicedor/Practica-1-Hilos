# Ejercicio 1 – Paralelismo basado en tareas (Fork-Join)

**Computación Paralela y Distribuida – Universidad Nacional de Colombia**

Este README explica, paso a paso, **el Punto 1 del ejercicio**: calcular la suma de los recíprocos de un arreglo usando **dos tareas que se ejecutan en paralelo** con el framework Fork-Join de Java.

> Alcance de este README: solo el Punto 1 (`parArraySum`, 2 tareas). El Punto 2 (`parManyTaskArraySum`, N tareas) lo desarrollan otros integrantes del grupo.

---

## 1. ¿Qué problema se resuelve?

Dado un arreglo `A`, hay que calcular:

```
suma = 1/A[0] + 1/A[1] + 1/A[2] + ... + 1/A[n-1]
```

La versión secuencial (ya incluida en el proyecto, `seqArraySum`) recorre el arreglo con un solo `for`, un elemento tras otro.

La clave para paralelizar: **el recíproco de cada elemento no depende del de los demás**. Por eso podemos repartir el arreglo entre varios trabajadores y juntar los resultados al final.

---

## 2. La idea en un dibujo

```
arreglo:   [ ............ | ............ ]
             mitad izquierda   mitad derecha
             tarea "left"      tarea "right"
                (hilo A)          (hilo B)       <- corren a la vez
                     \             /
                      left + right  = resultado final
```

1. Se calcula el punto medio: `mid = input.length / 2`.
2. Se crean **dos tareas**: una suma de `0` a `mid`, la otra de `mid` al final.
3. Se **lanzan en paralelo** en un grupo de hilos (*pool*).
4. Se **espera** a que ambas terminen.
5. Se **suman los dos resultados parciales**.

---

## 3. Conceptos que se usan

| Concepto | Qué significa |
|---|---|
| **Tarea** (`RecursiveAction`) | Un trabajo pequeño e independiente. Aquí: "suma los recíprocos de este rango". |
| **Pool** (`ForkJoinPool`) | Un grupo de hilos trabajadores ya creados, esperando trabajo. `new ForkJoinPool(2)` = 2 hilos. |
| **Fork** | Mandar una tarea al pool para que un hilo la ejecute **sin esperar** su resultado. |
| **Join** | Esperar a que una tarea termine, para poder leer su resultado de forma segura. |
| **Speedup** | Cuántas veces más rápido es la versión paralela que la secuencial. |

---

## 4. Qué se cambió en el código (antes y después)

Todo el trabajo está en **un solo archivo**:

`ejercicio_1/src/main/java/co/edu/unal/paralela/ReciprocalArraySum.java`

No se cambiaron las firmas de los métodos públicos ni protegidos, como pide la guía. Son **3 cambios**.

### Cambio 1 – Importar `ForkJoinPool`

**Antes:**
```java
import java.util.concurrent.RecursiveAction;
```

**Después:**
```java
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
```

Permite usar el pool de hilos.

### Cambio 2 – Implementar `compute()` de la tarea

`compute()` es lo que hace **cada tarea** cuando le toca ejecutarse. Estaba vacío.

**Antes:**
```java
@Override
protected void compute() {
    // Para hacer
}
```

**Después:**
```java
@Override
protected void compute() {
    double sum = 0;
    for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
        sum += 1 / input[i];
    }
    value = sum;
}
```

Es el mismo `for` de la versión secuencial, pero recorriendo **solo el rango de la tarea** (`startIndexInclusive` hasta `endIndexExclusive`). El total queda en `value`, y se lee después con `getValue()`.

Se suma en una variable local (`sum`) y se guarda en `value` al final, porque escribir en una variable local es más rápido que escribir en un campo en cada vuelta del ciclo.

### Cambio 3 – Implementar `parArraySum`

**Antes** (solo copiaba la versión secuencial):
```java
protected static double parArraySum(final double[] input) {
    assert input.length % 2 == 0;

    double sum = 0;
    for (int i = 0; i < input.length; i++) {
        sum += 1 / input[i];
    }
    return sum;
}
```

**Después:**
```java
// Pool de 2 hilos, creado UNA sola vez y reutilizado (campo de la clase)
private static final ForkJoinPool POOL_2 = new ForkJoinPool(2);

protected static double parArraySum(final double[] input) {
    assert input.length % 2 == 0;

    final int mid = input.length / 2;

    // Dos tareas: una por cada mitad del arreglo
    final ReciprocalArraySumTask left  = new ReciprocalArraySumTask(0, mid, input);
    final ReciprocalArraySumTask right = new ReciprocalArraySumTask(mid, input.length, input);

    POOL_2.execute(left);    // fork: "left" corre en un hilo del pool, sin esperar
    POOL_2.invoke(right);    // "right" corre en paralelo y se espera a que termine
    left.join();             // join: esperar a que "left" termine

    return left.getValue() + right.getValue();
}
```

Línea por línea:

- `mid = input.length / 2` – punto medio del arreglo.
- `new ReciprocalArraySumTask(0, mid, input)` – tarea para la **primera mitad**.
- `new ReciprocalArraySumTask(mid, input.length, input)` – tarea para la **segunda mitad**.
- `POOL_2.execute(left)` – **fork**: se entrega `left` al pool y el programa sigue sin esperar.
- `POOL_2.invoke(right)` – se ejecuta `right` y se espera a que termine. Mientras tanto, `left` ya está corriendo al mismo tiempo: eso es el paralelismo.
- `left.join()` – espera a que `left` termine. Sin esto, `getValue()` podría leerse antes de tiempo y dar un resultado incompleto.
- `left.getValue() + right.getValue()` – se combinan los dos resultados parciales.

---

## 5. ¿Por qué un pool que se reutiliza?

En una primera versión se creaba un `ForkJoinPool` **nuevo en cada llamada** y se cerraba al final. Funcionaba, pero en arreglos pequeños (2 millones de elementos) la suma dura cerca de 1 ms y **crear los hilos cuesta casi lo mismo**, así que no se ganaba tiempo. La prueba llama al método 60 veces, y ese costo se pagaba 60 veces.

La mejora fue crear el pool **una sola vez** (`POOL_2`) y reutilizarlo. La lógica no cambia: solo se deja de pagar el costo de crear hilos en cada llamada.

> Analogía: antes se contrataban 2 trabajadores cada vez que había que sumar un arreglo y se despedían al terminar. Ahora se contratan una vez y, cuando llega un arreglo, solo se les entrega el trabajo.

No hace falta `shutdown()` porque los hilos de un `ForkJoinPool` son *daemon* y no impiden que el programa termine.

---

## 6. ¿Qué pasa si se quita `left.join()`?

El programa terminaría, pero el resultado podría salir **incompleto y distinto en cada ejecución**: el hilo principal leería `left.getValue()` antes de que `left` terminara (todavía en 0). Es una condición de carrera. `join()` garantiza el orden: primero termina la tarea, después se lee su valor.

---

## 7. Cómo ejecutar y probar

Desde la carpeta que contiene el `pom.xml` (`ejercicio_1/`):

```
mvn test
```

Las pruebas (`ReciprocalArraySumTest`) hacen dos cosas con cada método:

1. Verifican que el resultado coincide con la versión secuencial (error menor a 0.01).
2. Miden el *speedup* contra la versión secuencial.

Para `parArraySum` (Punto 1) el speedup mínimo exigido es **1.5x**.

**Resultado obtenido:** `testParSimpleTwoMillion` y `testParSimpleTwoHundredMillion` (las de `parArraySum`) **pasan**. Las pruebas `testParManyTask...` corresponden al Punto 2 (`parManyTaskArraySum`) y quedan a cargo de otros integrantes.

> Nota: la prueba de 200 millones de elementos crea un arreglo de ~1.6 GB y puede tardar más de un minuto. Los tiempos varían según la carga del computador.

---

## 8. Preguntas de sustentación

**¿Qué hace `compute()`?**
Suma los recíprocos de su rango del arreglo y guarda el total en `value`.

**¿Por qué se parte el arreglo en dos mitades?**
Cada mitad es independiente, así que dos hilos pueden sumarlas a la vez y luego se combinan los resultados.

**¿Qué hace `execute` y qué hace `invoke`?**
`execute` entrega la tarea al pool y sigue sin esperar. `invoke` la ejecuta y espera a que termine.

**¿Qué hace `join()`?**
Espera a que la tarea termine para poder leer su resultado de forma segura.

**¿Qué es un pool?**
Un grupo de hilos reutilizables. Al mandar una tarea al pool, un hilo libre la ejecuta.

**¿Por qué reutilizar el pool?**
Crear hilos es costoso. En problemas pequeños ese costo puede ser comparable al trabajo útil, y reutilizar el pool lo evita.

**¿Por qué el speedup no es exactamente el número de hilos?**
Hay costo de crear y coordinar tareas, memoria compartida y trabajo que no se reparte de forma perfectamente pareja.
