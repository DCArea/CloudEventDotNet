namespace CloudEventDotNet;

/// <summary>
/// DeadLetter atribute
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class DeadLetterAttribute : Attribute
{
}
